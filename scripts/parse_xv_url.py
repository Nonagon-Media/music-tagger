#!/usr/bin/env python3
"""
Parse XVideos URLs to extract metadata and generate YAML for media processing.
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


def parse_xvideos_url(url):
    """Parse an XVideos URL and extract metadata."""
    html = fetch_page(url)

    # Extract title
    title_match = re.search(r'<title>([^<]+)</title>', html)
    title = None
    if title_match:
        title = title_match.group(1).replace(" - XVIDEOS.COM", "").strip()
        # Clean up HTML entities
        title = title.replace("&apos;", "'").replace("&amp;", "&").replace("&#039;", "'")

    # Extract uploader from uploader_url
    uploader_match = re.search(r'"uploader_url"\s*:\s*"\\?/?([^"\\]+)"', html)
    uploader = None
    if uploader_match:
        uploader = uploader_match.group(1)

    # Check if it's a verified channel/pornstar or amateur
    # Look for verified badge or professional channel indicators
    is_professional = bool(re.search(r'verified-channel|is-verified|pornstar-page', html, re.I))

    # For XV, most content is amateur unless from verified channels
    # Use uploader as artist, default album to "Amateur" or "XVideos"
    artist = uploader
    album = "Amateur"

    return {
        "url": url,
        "artist": artist,
        "album": album,
        "title": title,
        "dest_path": "adult/xv/auto",
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
    parser = argparse.ArgumentParser(description="Parse XVideos URLs and generate YAML")
    parser.add_argument("urls", nargs="+", help="One or more XVideos URLs to parse")
    parser.add_argument("-o", "--output", help="Output YAML file path (default: stdout)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and display without writing file")

    args = parser.parse_args()

    entries = []
    for url in args.urls:
        if "xvideos.com" not in url:
            print(f"Warning: Skipping non-XVideos URL: {url}", file=sys.stderr)
            continue

        print(f"Parsing: {url}", file=sys.stderr)
        result = parse_xvideos_url(url)
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
