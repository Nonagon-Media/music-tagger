"""
Music Tagger Review UI

A simple Flask web interface for reviewing and approving metadata matches.
"""

import os
import json
import sqlite3
from pathlib import Path

from flask import Flask, render_template, request, jsonify, redirect, url_for
from redis import Redis
from rq import Queue

app = Flask(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
CONFIDENCE_THRESHOLD = int(os.environ.get("CONFIDENCE_THRESHOLD", 80))
DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "music_tagger.db"


def get_db():
    """Get database connection."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def init_db():
    """Initialize database schema."""
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            queue TEXT,
            status TEXT,
            confidence REAL,
            current_meta TEXT,
            matched_meta TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_queue_status ON jobs(queue, status);
        CREATE INDEX IF NOT EXISTS idx_path ON jobs(path);
    """)
    db.commit()
    db.close()


def get_queue_stats():
    """Get counts for each queue."""
    db = get_db()
    stats = {}

    for queue in ["analysis", "review", "processing", "failed"]:
        count = db.execute(
            "SELECT COUNT(*) as count FROM jobs WHERE queue = ? AND status = 'pending'",
            (queue,)
        ).fetchone()["count"]
        stats[queue] = count

    stats["done"] = db.execute(
        "SELECT COUNT(*) as count FROM jobs WHERE status = 'done'"
    ).fetchone()["count"]

    stats["rejected"] = db.execute(
        "SELECT COUNT(*) as count FROM jobs WHERE status = 'rejected'"
    ).fetchone()["count"]

    db.close()
    return stats


@app.route("/")
def index():
    """Dashboard showing queue statistics."""
    stats = get_queue_stats()
    return render_template("index.html", stats=stats, threshold=CONFIDENCE_THRESHOLD)


@app.route("/queue/<queue_name>")
def view_queue(queue_name):
    """View items in a specific queue."""
    page = int(request.args.get("page", 1))
    per_page = 20
    offset = (page - 1) * per_page

    db = get_db()

    if queue_name == "done":
        jobs = db.execute("""
            SELECT * FROM jobs WHERE status = 'done'
            ORDER BY updated_at DESC LIMIT ? OFFSET ?
        """, (per_page, offset)).fetchall()
        total = db.execute("SELECT COUNT(*) as count FROM jobs WHERE status = 'done'").fetchone()["count"]
    elif queue_name == "rejected":
        jobs = db.execute("""
            SELECT * FROM jobs WHERE status = 'rejected'
            ORDER BY updated_at DESC LIMIT ? OFFSET ?
        """, (per_page, offset)).fetchall()
        total = db.execute("SELECT COUNT(*) as count FROM jobs WHERE status = 'rejected'").fetchone()["count"]
    else:
        jobs = db.execute("""
            SELECT * FROM jobs WHERE queue = ?
            ORDER BY confidence DESC, created_at ASC LIMIT ? OFFSET ?
        """, (queue_name, per_page, offset)).fetchall()
        total = db.execute(
            "SELECT COUNT(*) as count FROM jobs WHERE queue = ?",
            (queue_name,)
        ).fetchone()["count"]

    db.close()

    # Parse JSON fields
    jobs_list = []
    for job in jobs:
        job_dict = dict(job)
        job_dict["current_meta"] = json.loads(job["current_meta"]) if job["current_meta"] else {}
        job_dict["matched_meta"] = json.loads(job["matched_meta"]) if job["matched_meta"] else {}
        jobs_list.append(job_dict)

    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "queue.html",
        queue_name=queue_name,
        jobs=jobs_list,
        page=page,
        total_pages=total_pages,
        total=total
    )


@app.route("/job/<int:job_id>")
def view_job(job_id):
    """View details of a single job."""
    db = get_db()
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    db.close()

    if not job:
        return "Job not found", 404

    job_dict = dict(job)
    job_dict["current_meta"] = json.loads(job["current_meta"]) if job["current_meta"] else {}
    job_dict["matched_meta"] = json.loads(job["matched_meta"]) if job["matched_meta"] else {}

    return render_template("job.html", job=job_dict)


@app.route("/api/approve/<int:job_id>", methods=["POST"])
def approve_job(job_id):
    """Approve a job and move it to processing queue."""
    db = get_db()

    # Update job
    db.execute("""
        UPDATE jobs SET
            queue = 'processing',
            status = 'pending',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND queue = 'review'
    """, (job_id,))
    db.commit()

    # Get job path for the task
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    db.close()

    if job:
        # Enqueue for processing
        redis_conn = Redis.from_url(REDIS_URL)
        q = Queue("processing", connection=redis_conn)
        # Import here to avoid circular imports
        import sys
        sys.path.insert(0, "/app")
        from tasks import write_tags
        q.enqueue(write_tags, job_id)

    return jsonify({"status": "approved", "job_id": job_id})


@app.route("/api/reject/<int:job_id>", methods=["POST"])
def reject_job(job_id):
    """Reject a job."""
    db = get_db()
    db.execute("""
        UPDATE jobs SET
            status = 'rejected',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND queue = 'review'
    """, (job_id,))
    db.commit()
    db.close()

    return jsonify({"status": "rejected", "job_id": job_id})


@app.route("/api/bulk-approve", methods=["POST"])
def bulk_approve():
    """Approve multiple jobs at once."""
    job_ids = request.json.get("job_ids", [])

    db = get_db()
    redis_conn = Redis.from_url(REDIS_URL)
    q = Queue("processing", connection=redis_conn)

    approved = 0
    for job_id in job_ids:
        db.execute("""
            UPDATE jobs SET
                queue = 'processing',
                status = 'pending',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND queue = 'review'
        """, (job_id,))

        import sys
        sys.path.insert(0, "/app")
        from tasks import write_tags
        q.enqueue(write_tags, job_id)
        approved += 1

    db.commit()
    db.close()

    return jsonify({"status": "approved", "count": approved})


@app.route("/api/bulk-reject", methods=["POST"])
def bulk_reject():
    """Reject multiple jobs at once."""
    job_ids = request.json.get("job_ids", [])

    db = get_db()
    for job_id in job_ids:
        db.execute("""
            UPDATE jobs SET
                status = 'rejected',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND queue = 'review'
        """, (job_id,))

    db.commit()
    db.close()

    return jsonify({"status": "rejected", "count": len(job_ids)})


@app.route("/api/retry/<int:job_id>", methods=["POST"])
def retry_job(job_id):
    """Retry a failed job."""
    db = get_db()

    db.execute("""
        UPDATE jobs SET
            queue = 'analysis',
            status = 'pending',
            error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND queue = 'failed'
    """, (job_id,))
    db.commit()

    # Re-enqueue
    redis_conn = Redis.from_url(REDIS_URL)
    q = Queue("analysis", connection=redis_conn)

    import sys
    sys.path.insert(0, "/app")
    from tasks import analyze_track
    q.enqueue(analyze_track, job_id)

    db.close()

    return jsonify({"status": "retried", "job_id": job_id})


@app.route("/api/stats")
def api_stats():
    """API endpoint for queue statistics."""
    return jsonify(get_queue_stats())


if __name__ == "__main__":
    # Initialize database on startup
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=False)
