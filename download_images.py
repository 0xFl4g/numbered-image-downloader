#!/usr/bin/env python3
"""
Image downloader script that iterates through numbered URLs and downloads images.
"""

import argparse
import json
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Event
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

# Global shutdown event
shutdown_event = Event()


def load_config(config_path: str) -> dict:
    """Load and validate configuration from JSON file."""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        sys.exit(f"Error: Config file '{config_path}' not found.\n"
                 f"Copy config.example.json to {config_path} and edit it.")
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Invalid JSON in '{config_path}': {e}")

    required = ["base_url", "start", "end"]
    missing = [key for key in required if key not in config]
    if missing:
        sys.exit(f"Error: Missing required config keys: {', '.join(missing)}")

    if "{" not in config["base_url"]:
        sys.exit("Error: base_url must contain a format placeholder (e.g., {:04d})")

    return config


class ImageDownloader:
    def __init__(self, timeout: int, max_retries: int, pool_size: int):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        # Configure connection pool to match concurrent workers
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            pool_block=True  # Block when pool is full instead of creating new connections
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.timeout = timeout
        self.max_retries = max_retries

    def download(self, url: str, output_path: Path) -> tuple[str, bool, str]:
        """
        Download a single image with streaming.
        Returns: (url, success, message)
        """
        if shutdown_event.is_set():
            return (url, False, "skipped")

        temp_path = output_path.with_suffix(".tmp")

        for attempt in range(self.max_retries):
            if shutdown_event.is_set():
                return (url, False, "skipped")
            try:
                response = self.session.get(url, timeout=self.timeout, stream=True)

                if response.status_code == 200:
                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                            if shutdown_event.is_set():
                                temp_path.unlink(missing_ok=True)
                                return (url, False, "skipped")
                            f.write(chunk)
                    # Atomic rename - only complete files get the final name
                    temp_path.rename(output_path)
                    return (url, True, f"Downloaded: {output_path.name}")
                elif response.status_code == 404:
                    return (url, False, "")  # Silent for 404s
                else:
                    temp_path.unlink(missing_ok=True)
                    if attempt == self.max_retries - 1:
                        return (url, False, f"HTTP {response.status_code}: {output_path.name}")
            except requests.RequestException as e:
                temp_path.unlink(missing_ok=True)
                if attempt == self.max_retries - 1:
                    return (url, False, f"Error: {output_path.name} - {e}")
            time.sleep(0.5 * (attempt + 1))  # Exponential backoff

        return (url, False, f"Failed after {self.max_retries} retries: {output_path.name}")

    def close(self):
        self.session.close()


def main():
    parser = argparse.ArgumentParser(description="Download numbered images from URLs")
    parser.add_argument("-c", "--config", default="config.json",
                        help="Path to config file (default: config.json)")
    args = parser.parse_args()

    config = load_config(args.config)

    base_url = config["base_url"]
    start = config["start"]
    end = config["end"]
    output_dir = Path(config.get("output_dir", "images"))
    timeout = config.get("timeout", 30)
    max_retries = config.get("max_retries", 3)
    concurrent = config.get("concurrent_downloads", 10)

    # Derive filename pattern from URL
    filename_pattern = Path(urlparse(base_url).path).name

    # Set up graceful shutdown
    def signal_handler(signum, frame):
        print("\n\nShutting down gracefully... (press Ctrl+C again to force quit)")
        shutdown_event.set()
        # Restore default handler for force quit
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, OSError):
        pass  # SIGTERM not available on Windows

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading images from {start} to {end}")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"Concurrent downloads: {concurrent}")
    print("Press Ctrl+C to stop gracefully")
    print("-" * 50)

    # Prepare download tasks (skip existing files)
    tasks = []
    for i in range(start, end + 1):
        url = base_url.format(i)
        filename = filename_pattern.format(i)
        output_path = output_dir / filename

        if not output_path.exists():
            tasks.append((url, output_path))

    if not tasks:
        print("All images already downloaded!")
        return

    print(f"Images to download: {len(tasks)}")
    print("-" * 50)

    downloader = ImageDownloader(timeout, max_retries, concurrent)
    downloaded = 0
    failed = 0
    not_found = 0
    skipped = 0

    try:
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            futures = {
                executor.submit(downloader.download, url, path): (url, path)
                for url, path in tasks
            }

            for future in as_completed(futures):
                if shutdown_event.is_set():
                    # Cancel pending futures
                    for f in futures:
                        f.cancel()
                    break

                url, success, message = future.result()
                total_processed = downloaded + failed + not_found + skipped

                if success:
                    downloaded += 1
                    print(f"[{total_processed + 1}/{len(tasks)}] {message}")
                elif message == "":  # 404
                    not_found += 1
                elif message == "skipped":
                    skipped += 1
                else:
                    failed += 1
                    print(f"[{total_processed + 1}/{len(tasks)}] {message}")

    finally:
        downloader.close()

    print("-" * 50)
    if shutdown_event.is_set():
        remaining = len(tasks) - (downloaded + failed + not_found + skipped)
        print(f"Stopped! Downloaded: {downloaded}, Not found: {not_found}, Failed: {failed}, Remaining: {remaining}")
        print("Run again to resume.")
    else:
        print(f"Complete! Downloaded: {downloaded}, Not found: {not_found}, Failed: {failed}")


if __name__ == "__main__":
    main()
