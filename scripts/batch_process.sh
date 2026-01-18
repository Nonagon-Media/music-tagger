#!/bin/bash
#
# Batch process music library one artist at a time
# Run with: nohup ./batch_process.sh > /dev/null 2>&1 &
# Check progress: tail -f /opt/music-tagger/logs/batch_process.log
#

set -e

# Configuration
MUSIC_DIR="/music"
LOG_FILE="/opt/music-tagger/logs/batch_process.log"
DELAY_BETWEEN_ARTISTS=180  # 3 minutes between artists
POLL_INTERVAL=30           # Check job status every 30 seconds
DATA_DIR="/data"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

get_queue_depth() {
    # Check Redis queue directly - more reliable than SQLite status
    redis-cli -h redis LLEN rq:queue:analysis 2>/dev/null || echo "0"
}

get_artist_track_count() {
    local artist="$1"
    find "$MUSIC_DIR/$artist" -type f \( -name "*.mp3" -o -name "*.flac" -o -name "*.m4a" \) 2>/dev/null | wc -l
}

get_artist_processed_count() {
    local artist="$1"
    sqlite3 "$DATA_DIR/music_tagger.db" \
        "SELECT COUNT(*) FROM jobs WHERE path LIKE '%/$artist/%' AND status = 'done';" 2>/dev/null || echo "0"
}

wait_for_jobs() {
    log "Waiting for current jobs to complete..."
    # Small delay to let jobs get queued
    sleep 5
    while true; do
        queue_depth=$(get_queue_depth)
        if [ "$queue_depth" -eq 0 ]; then
            break
        fi
        log "  $queue_depth jobs in analysis queue..."
        sleep "$POLL_INTERVAL"
    done
    log "All jobs completed"
}

process_artist() {
    local artist="$1"

    local total_tracks=$(get_artist_track_count "$artist")
    local processed=$(get_artist_processed_count "$artist")

    if [ "$total_tracks" -eq 0 ]; then
        log "SKIP: '$artist' - no audio files found"
        return 0
    fi

    if [ "$processed" -ge "$total_tracks" ]; then
        log "SKIP: '$artist' - already processed ($processed/$total_tracks tracks)"
        return 0
    fi

    log "PROCESSING: '$artist' ($processed/$total_tracks already done)"

    # Run the seeder for this artist
    cd /app
    python seed.py --artist "$artist" 2>&1 | while read line; do
        log "  $line"
    done

    # Wait for all jobs to complete
    wait_for_jobs

    # Report results
    local new_processed=$(get_artist_processed_count "$artist")
    log "DONE: '$artist' - $new_processed/$total_tracks tracks processed"
}

main() {
    log "========================================="
    log "Starting batch processing"
    log "Music directory: $MUSIC_DIR"
    log "Delay between artists: ${DELAY_BETWEEN_ARTISTS}s"
    log "========================================="

    # Get list of artist folders
    local artists=()
    while IFS= read -r artist; do
        artists+=("$artist")
    done < <(ls -1 "$MUSIC_DIR" | sort)

    local total_artists=${#artists[@]}
    log "Found $total_artists artist folders"

    local count=0
    for artist in "${artists[@]}"; do
        count=$((count + 1))
        log ""
        log "[$count/$total_artists] Processing: $artist"

        process_artist "$artist"

        # Delay between artists (unless last one)
        if [ "$count" -lt "$total_artists" ]; then
            log "Waiting ${DELAY_BETWEEN_ARTISTS}s before next artist..."
            sleep "$DELAY_BETWEEN_ARTISTS"
        fi
    done

    log ""
    log "========================================="
    log "Batch processing complete!"
    log "========================================="
}

# Run main
main "$@"
