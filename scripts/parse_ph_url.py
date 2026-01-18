#!/usr/bin/env python3
"""
Parse Pornhub URLs to extract metadata and generate YAML for media processing.
"""

import sys
import re
import subprocess
import argparse
from datetime import datetime
from pathlib import Path


def fetch_page(url):
    """Fetch page content using curl."""
    result = subprocess.run(
        ["curl", "-s", "-L", "-A",
         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
         url],
        capture_output=True,
        text=True,
        timeout=30
    )
    return result.stdout


def parse_pornhub_url(url):
    """Parse a Pornhub URL and extract metadata."""
    html = fetch_page(url)

    # Extract pornstar/artist from data-context-pornstar
    artist_match = re.search(r"data-context-pornstar='([^']+)'", html)
    artist = None
    if artist_match:
        # Convert "Lily-Adams" to "Lily Adams"
        artist = artist_match.group(1).replace("-", " ")

    # Extract studio/album from "author" field
    author_match = re.search(r'"author"\s*:\s*"([^"]+)"', html)
    album = None
    author = None
    if author_match:
        author = author_match.group(1)

    # If no pornstar (amateur content), use author/uploader as artist
    if not artist and author:
        artist = author
        album = "Amateur"
    else:
        # For professional content, author is the studio/album
        album = author

    # Fallback: try to get album from tags if still not set
    if not album:
        tag_match = re.search(r"data-context-tag='([^']+)'", html)
        if tag_match:
            tags = tag_match.group(1).split(",")
            if tags:
                album = tags[0].replace("-", " ").title()

    # Extract title for reference
    title_match = re.search(r'<title>([^<]+)</title>', html)
    title = None
    if title_match:
        title = title_match.group(1).replace(" | Pornhub", "").strip()
        # Clean up HTML entities
        title = title.replace("&#124;", "|").replace("&amp;", "&")

    return {
        "url": url,
        "artist": artist,
        "album": album,
        "title": title,
        "dest_path": "adult/ph/auto",
        "archive_dir": "adult"
    }


def generate_yaml(entries):
    """Generate YAML content from parsed entries."""
    lines = [
        "# Auto-generated media download queue",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "#",
        "processed: false",
        "",
        "downloads:"
    ]

    for entry in entries:
        lines.append(f"  - url: {entry['url']}")
        if entry.get('artist'):
            lines.append(f"    artist: {entry['artist']}")
        if entry.get('album'):
            lines.append(f"    album: {entry['album']}")
        lines.append(f"    dest_path: {entry['dest_path']}")
        lines.append(f"    archive_dir: {entry['archive_dir']}")
        if entry.get('title'):
            # Add title as comment for reference
            lines.append(f"    # title: {entry['title']}")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Parse Pornhub URLs and generate YAML")
    parser.add_argument("urls", nargs="+", help="One or more Pornhub URLs to parse")
    parser.add_argument("-o", "--output", help="Output YAML file path (default: stdout)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and display without writing file")

    args = parser.parse_args()

    entries = []
    for url in args.urls:
        if "pornhub.com" not in url:
            print(f"Warning: Skipping non-Pornhub URL: {url}", file=sys.stderr)
            continue

        print(f"Parsing: {url}", file=sys.stderr)
        result = parse_pornhub_url(url)
        entries.append(result)

        print(f"  Artist: {result['artist']}", file=sys.stderr)
        print(f"  Album:  {result['album']}", file=sys.stderr)
        print(f"  Title:  {result['title']}", file=sys.stderr)

    if not entries:
        print("Error: No valid URLs to process", file=sys.stderr)
        sys.exit(1)

    yaml_content = generate_yaml(entries)

    if args.dry_run or not args.output:
        print("\n--- Generated YAML ---")
        print(yaml_content)
    else:
        output_path = Path(args.output)
        output_path.write_text(yaml_content)
        print(f"\nYAML written to: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
