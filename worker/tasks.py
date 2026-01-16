"""
Music Tagger Worker Tasks

Handles fingerprinting, MusicBrainz lookups, and tag writing.
"""

import os
import json
import time
import sqlite3
import subprocess
from pathlib import Path

import acoustid
import musicbrainzngs
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC
from mutagen.mp4 import MP4
from redis import Redis
from rq import Queue

# Configure MusicBrainz
musicbrainzngs.set_useragent("MusicTagger", "1.0", "https://github.com/Nonagon-Media/music-tagger")

# Environment config
ACOUSTID_API_KEY = os.environ.get("ACOUSTID_API_KEY")
CONFIDENCE_THRESHOLD = int(os.environ.get("CONFIDENCE_THRESHOLD", 80))
RATE_LIMIT_DELAY = float(os.environ.get("RATE_LIMIT_DELAY", 0.5))
WRITE_DELAY = float(os.environ.get("WRITE_DELAY", 0.5))
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DATA_DIR = Path("/data")

# Database setup
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


def get_current_metadata(filepath):
    """Extract current metadata from audio file."""
    filepath = Path(filepath)
    ext = filepath.suffix.lower()

    try:
        if ext == ".mp3":
            audio = EasyID3(filepath)
        elif ext == ".flac":
            audio = FLAC(filepath)
        elif ext in (".m4a", ".mp4"):
            audio = MP4(filepath)
            # Normalize MP4 tags
            return {
                "artist": audio.tags.get("\xa9ART", [""])[0] if audio.tags else "",
                "album": audio.tags.get("\xa9alb", [""])[0] if audio.tags else "",
                "title": audio.tags.get("\xa9nam", [""])[0] if audio.tags else "",
                "tracknumber": str(audio.tags.get("trkn", [(0, 0)])[0][0]) if audio.tags else "",
                "date": audio.tags.get("\xa9day", [""])[0] if audio.tags else "",
            }
        else:
            return None

        return {
            "artist": audio.get("artist", [""])[0],
            "album": audio.get("album", [""])[0],
            "title": audio.get("title", [""])[0],
            "tracknumber": audio.get("tracknumber", [""])[0],
            "date": audio.get("date", [""])[0],
        }
    except Exception as e:
        return {"error": str(e)}


def fingerprint_file(filepath):
    """Generate acoustic fingerprint using fpcalc."""
    try:
        result = subprocess.run(
            ["fpcalc", "-json", filepath],
            capture_output=True,
            text=True,
            timeout=60
        )
        # Try parsing output even if exit code is non-zero
        # fpcalc sometimes returns errors but still produces valid fingerprints
        if result.stdout:
            data = json.loads(result.stdout)
            fingerprint = data.get("fingerprint")
            duration = data.get("duration")
            if fingerprint and duration:
                return fingerprint, duration
    except Exception as e:
        return None, None
    return None, None


def lookup_acoustid(fingerprint, duration):
    """Look up fingerprint in AcoustID database."""
    if not ACOUSTID_API_KEY:
        raise ValueError("ACOUSTID_API_KEY not set")

    try:
        response = acoustid.lookup(
            ACOUSTID_API_KEY,
            fingerprint,
            duration,
            meta="recordings releasegroups"
        )
        # acoustid.lookup() returns a dict with 'results' and 'status' keys
        return response.get("results", [])
    except Exception as e:
        return []


def get_musicbrainz_release(release_group_id):
    """Get release details from MusicBrainz."""
    try:
        result = musicbrainzngs.get_release_group_by_id(
            release_group_id,
            includes=["artists", "releases"]
        )
        return result.get("release-group")
    except Exception as e:
        return None


def analyze_track(job_id):
    """
    Analyze a single track: fingerprint, lookup, and route based on confidence.
    """
    init_db()
    db = get_db()

    # Get job
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        return {"error": "Job not found"}

    filepath = job["path"]

    # Update status
    db.execute(
        "UPDATE jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (job_id,)
    )
    db.commit()

    try:
        # Get current metadata
        current_meta = get_current_metadata(filepath)

        # Generate fingerprint
        fingerprint, duration = fingerprint_file(filepath)
        if not fingerprint:
            raise Exception("Failed to generate fingerprint")

        time.sleep(RATE_LIMIT_DELAY)  # Rate limit

        # Lookup in AcoustID
        results = lookup_acoustid(fingerprint, duration)

        if not results:
            raise Exception("No AcoustID matches found")

        # Find best match
        best_match = None
        best_score = 0

        for result in results:
            # Skip non-dict results (API sometimes returns strings or other types)
            if not isinstance(result, dict):
                continue
            score = result.get("score", 0) * 100
            if score > best_score and "recordings" in result:
                for recording in result["recordings"]:
                    if "releasegroups" in recording:
                        best_match = {
                            "score": score,
                            "recording": recording,
                            "title": recording.get("title"),
                            "artists": [a.get("name") for a in recording.get("artists", [])],
                            "releases": recording.get("releasegroups", [])
                        }
                        best_score = score
                        break

        if not best_match:
            raise Exception("No valid recordings found in AcoustID results")

        time.sleep(RATE_LIMIT_DELAY)  # Rate limit

        # Get MusicBrainz details for best release
        matched_meta = {
            "title": best_match.get("title"),
            "artist": ", ".join(best_match.get("artists", [])),
            "confidence": best_score,
        }

        if best_match.get("releases"):
            release_group = best_match["releases"][0]
            matched_meta["album"] = release_group.get("title")
            matched_meta["release_group_id"] = release_group.get("id")

            # Get more details from MusicBrainz
            mb_details = get_musicbrainz_release(release_group.get("id"))
            if mb_details:
                matched_meta["date"] = mb_details.get("first-release-date", "")

        # Determine target queue based on confidence
        confidence = best_score
        if confidence >= CONFIDENCE_THRESHOLD:
            target_queue = "processing"
        else:
            target_queue = "review"

        # Update job
        db.execute("""
            UPDATE jobs SET
                queue = ?,
                status = 'pending',
                confidence = ?,
                current_meta = ?,
                matched_meta = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            target_queue,
            confidence,
            json.dumps(current_meta),
            json.dumps(matched_meta),
            job_id
        ))
        db.commit()

        # If high confidence, enqueue for processing
        if target_queue == "processing":
            redis_conn = Redis.from_url(REDIS_URL)
            q = Queue("processing", connection=redis_conn)
            q.enqueue(write_tags, job_id)

        return {
            "status": "success",
            "confidence": confidence,
            "queue": target_queue,
            "matched": matched_meta
        }

    except Exception as e:
        db.execute("""
            UPDATE jobs SET
                queue = 'failed',
                status = 'failed',
                error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (str(e), job_id))
        db.commit()
        return {"status": "error", "error": str(e)}
    finally:
        db.close()


def write_tags(job_id):
    """
    Write matched metadata to audio file.
    """
    init_db()
    db = get_db()

    job = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not job:
        return {"error": "Job not found"}

    filepath = Path(job["path"])
    matched_meta = json.loads(job["matched_meta"]) if job["matched_meta"] else {}

    db.execute(
        "UPDATE jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (job_id,)
    )
    db.commit()

    try:
        ext = filepath.suffix.lower()

        if ext == ".mp3":
            audio = EasyID3(filepath)
        elif ext == ".flac":
            audio = FLAC(filepath)
        elif ext in (".m4a", ".mp4"):
            audio = MP4(filepath)
            # MP4 uses different tag names
            if matched_meta.get("title"):
                audio["\xa9nam"] = [matched_meta["title"]]
            if matched_meta.get("artist"):
                audio["\xa9ART"] = [matched_meta["artist"]]
            if matched_meta.get("album"):
                audio["\xa9alb"] = [matched_meta["album"]]
            if matched_meta.get("date"):
                audio["\xa9day"] = [matched_meta["date"]]
            audio.save()

            db.execute("""
                UPDATE jobs SET
                    queue = 'done',
                    status = 'done',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (job_id,))
            db.commit()

            time.sleep(WRITE_DELAY)
            return {"status": "success", "path": str(filepath)}
        else:
            raise Exception(f"Unsupported file type: {ext}")

        # Update tags for ID3/FLAC
        if matched_meta.get("title"):
            audio["title"] = matched_meta["title"]
        if matched_meta.get("artist"):
            audio["artist"] = matched_meta["artist"]
        if matched_meta.get("album"):
            audio["album"] = matched_meta["album"]
        if matched_meta.get("date"):
            audio["date"] = matched_meta["date"]

        audio.save()

        db.execute("""
            UPDATE jobs SET
                queue = 'done',
                status = 'done',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (job_id,))
        db.commit()

        time.sleep(WRITE_DELAY)
        return {"status": "success", "path": str(filepath)}

    except Exception as e:
        db.execute("""
            UPDATE jobs SET
                queue = 'failed',
                status = 'failed',
                error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (str(e), job_id))
        db.commit()
        return {"status": "error", "error": str(e)}
    finally:
        db.close()


def approve_job(job_id):
    """Move a job from review queue to processing queue."""
    init_db()
    db = get_db()

    db.execute("""
        UPDATE jobs SET
            queue = 'processing',
            status = 'pending',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND queue = 'review'
    """, (job_id,))
    db.commit()

    # Enqueue for processing
    redis_conn = Redis.from_url(REDIS_URL)
    q = Queue("processing", connection=redis_conn)
    q.enqueue(write_tags, job_id)

    db.close()
    return {"status": "approved", "job_id": job_id}


def reject_job(job_id):
    """Mark a job as rejected."""
    init_db()
    db = get_db()

    db.execute("""
        UPDATE jobs SET
            status = 'rejected',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND queue = 'review'
    """, (job_id,))
    db.commit()
    db.close()

    return {"status": "rejected", "job_id": job_id}
