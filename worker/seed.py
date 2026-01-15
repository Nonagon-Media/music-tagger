#!/usr/bin/env python3
"""
Seed script to populate the analysis queue with music files.

Usage:
    docker compose run --rm analysis-worker python seed.py [--artist "Artist Name"]
"""

import os
import sys
import argparse
import sqlite3
from pathlib import Path

from redis import Redis
from rq import Queue

from tasks import init_db, analyze_track, DATA_DIR, REDIS_URL

MUSIC_DIR = Path("/music")
SUPPORTED_EXTENSIONS = {".mp3", ".flac", ".m4a", ".mp4", ".ogg", ".wma"}
DB_PATH = DATA_DIR / "music_tagger.db"


def get_db():
    """Get database connection."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def find_music_files(base_path, artist_filter=None):
    """Find all music files in directory."""
    files = []

    # If artist filter specified, only scan that artist's folder
    if artist_filter:
        artist_path = base_path / artist_filter
        if not artist_path.exists():
            print(f"Warning: Artist folder not found: {artist_path}")
            return []
        scan_path = artist_path
    else:
        scan_path = base_path

    for root, dirs, filenames in os.walk(scan_path):
        for filename in filenames:
            if Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS:
                files.append(Path(root) / filename)

    return sorted(files)


def seed_queue(artist_filter=None, dry_run=False):
    """Seed the analysis queue with music files."""
    init_db()
    db = get_db()

    redis_conn = Redis.from_url(REDIS_URL)
    queue = Queue("analysis", connection=redis_conn)

    print(f"Scanning {MUSIC_DIR}...")
    if artist_filter:
        print(f"Filtering by artist: {artist_filter}")

    files = find_music_files(MUSIC_DIR, artist_filter)
    print(f"Found {len(files)} music files")

    added = 0
    skipped = 0

    for filepath in files:
        # Check if already in database
        existing = db.execute(
            "SELECT id, status FROM jobs WHERE path = ?",
            (str(filepath),)
        ).fetchone()

        if existing:
            if existing["status"] in ("done", "processing"):
                skipped += 1
                continue
            # Re-queue failed or pending jobs
            job_id = existing["id"]
        else:
            # Insert new job
            cursor = db.execute(
                "INSERT INTO jobs (path, queue, status) VALUES (?, 'analysis', 'pending')",
                (str(filepath),)
            )
            job_id = cursor.lastrowid
            db.commit()

        if dry_run:
            print(f"  Would queue: {filepath}")
        else:
            queue.enqueue(analyze_track, job_id)

        added += 1

        if added % 100 == 0:
            print(f"  Queued {added} files...")

    db.close()

    print(f"\nSummary:")
    print(f"  Added to queue: {added}")
    print(f"  Skipped (already processed): {skipped}")

    if dry_run:
        print("\n(Dry run - no jobs were actually queued)")


def main():
    parser = argparse.ArgumentParser(description="Seed the music tagger analysis queue")
    parser.add_argument(
        "--artist",
        type=str,
        help="Only process files from this artist (top-level directory name)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be queued without actually queueing"
    )

    args = parser.parse_args()

    if not MUSIC_DIR.exists():
        print(f"Error: Music directory not found at {MUSIC_DIR}")
        print("Make sure MUSIC_DIR is set in .env and the volume is mounted")
        sys.exit(1)

    seed_queue(artist_filter=args.artist, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
