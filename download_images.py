#!/usr/bin/env python3
"""
Async image downloader that iterates through numbered URLs and downloads images.
"""

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import signal
import sys
from pathlib import Path
from typing import TypedDict
from urllib.parse import urlparse

import aiohttp
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)


class Config(TypedDict, total=False):
    """Configuration schema for the downloader."""

    base_url: str  # Required
    start: int  # Required
    end: int  # Required
    output_dir: str
    timeout: int
    max_retries: int
    concurrent_downloads: int
    verify_checksums: bool


class DownloadResult:
    """Result of a download attempt."""

    __slots__ = ("bytes_downloaded", "message", "success", "url")

    def __init__(self, url: str, success: bool, message: str, bytes_downloaded: int = 0):
        self.url = url
        self.success = success
        self.message = message
        self.bytes_downloaded = bytes_downloaded


def validate_format_string(base_url: str, start: int) -> None:
    """Validate that the URL format string works with integer formatting."""
    try:
        test_url = base_url.format(start)
        if test_url == base_url:
            raise ValueError("Format placeholder not found or not replaced")
    except (IndexError, KeyError, ValueError) as e:
        sys.exit(
            f"Error: Invalid format placeholder in base_url.\n"
            f"  URL: {base_url}\n"
            f"  Use {{}} or {{:04d}} for the number placeholder.\n"
            f"  Details: {e}"
        )


def load_config(config_path: str) -> Config:
    """Load and validate configuration from JSON file."""
    try:
        with Path(config_path).open() as f:
            config: Config = json.load(f)
    except FileNotFoundError:
        sys.exit(
            f"Error: Config file '{config_path}' not found.\n"
            f"Copy config.example.json to {config_path} and edit it."
        )
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Invalid JSON in '{config_path}': {e}")

    required = ["base_url", "start", "end"]
    missing = [key for key in required if key not in config]
    if missing:
        sys.exit(f"Error: Missing required config keys: {', '.join(missing)}")

    if "{" not in config["base_url"]:
        sys.exit("Error: base_url must contain a format placeholder (e.g., {:04d})")

    validate_format_string(config["base_url"], config["start"])

    return config


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def load_checksums(checksum_file: Path) -> dict[str, str]:
    """Load checksums from manifest file."""
    if not checksum_file.exists():
        return {}
    try:
        with checksum_file.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_checksums(checksum_file: Path, checksums: dict[str, str]) -> None:
    """Save checksums to manifest file."""
    with checksum_file.open("w") as f:
        json.dump(checksums, f, indent=2)


def verify_existing_files(
    checksums: dict[str, str],
    files_to_check: list[Path],
) -> list[Path]:
    """Verify existing files against checksums. Returns list of corrupted files."""
    corrupted = []
    for file_path in files_to_check:
        if file_path.exists() and file_path.name in checksums:
            actual = compute_sha256(file_path)
            if actual != checksums[file_path.name]:
                corrupted.append(file_path)
    return corrupted


class AsyncImageDownloader:
    """Async image downloader using aiohttp."""

    def __init__(
        self,
        timeout: int,
        max_retries: int,
        concurrent: int,
        shutdown_event: asyncio.Event,
    ):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.concurrent = concurrent
        self.shutdown_event = shutdown_event
        self.connector = aiohttp.TCPConnector(limit=concurrent, limit_per_host=concurrent)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    async def download(
        self,
        session: aiohttp.ClientSession,
        url: str,
        output_path: Path,
    ) -> DownloadResult:
        """Download a single image with streaming."""
        if self.shutdown_event.is_set():
            return DownloadResult(url, False, "skipped")

        temp_path = output_path.with_suffix(".tmp")

        for attempt in range(self.max_retries):
            if self.shutdown_event.is_set():
                return DownloadResult(url, False, "skipped")
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        bytes_downloaded = 0
                        with temp_path.open("wb") as f:
                            async for chunk in response.content.iter_chunked(65536):
                                if self.shutdown_event.is_set():
                                    f.close()
                                    temp_path.unlink(missing_ok=True)
                                    return DownloadResult(url, False, "skipped")
                                f.write(chunk)
                                bytes_downloaded += len(chunk)
                        # Atomic rename
                        temp_path.rename(output_path)
                        return DownloadResult(
                            url, True, f"Downloaded: {output_path.name}", bytes_downloaded
                        )
                    elif response.status == 404:
                        return DownloadResult(url, False, "")  # Silent for 404s
                    else:
                        temp_path.unlink(missing_ok=True)
                        if attempt == self.max_retries - 1:
                            return DownloadResult(
                                url, False, f"HTTP {response.status}: {output_path.name}"
                            )
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                temp_path.unlink(missing_ok=True)
                if attempt == self.max_retries - 1:
                    return DownloadResult(url, False, f"Error: {output_path.name} - {e}")
            await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff

        return DownloadResult(
            url, False, f"Failed after {self.max_retries} retries: {output_path.name}"
        )

    async def download_all(
        self,
        tasks: list[tuple[str, Path]],
        checksums: dict[str, str],
        pbar_bytes: tqdm,
        pbar_imgs: tqdm,
    ) -> tuple[int, int, int, int, int]:
        """Download all images concurrently. Returns (downloaded, failed, not_found, skipped, total_bytes)."""
        downloaded = 0
        failed = 0
        not_found = 0
        skipped = 0
        total_bytes = 0

        async with aiohttp.ClientSession(
            connector=self.connector,
            timeout=self.timeout,
            headers=self.headers,
        ) as session:
            semaphore = asyncio.Semaphore(self.concurrent)

            async def bounded_download(url: str, path: Path) -> DownloadResult:
                async with semaphore:
                    return await self.download(session, url, path)

            pending = {asyncio.create_task(bounded_download(url, path)) for url, path in tasks}

            while pending:
                if self.shutdown_event.is_set():
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    break

                done, pending = await asyncio.wait(
                    pending,
                    timeout=0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    try:
                        result = task.result()
                    except asyncio.CancelledError:
                        skipped += 1
                        continue

                    total_bytes += result.bytes_downloaded

                    if result.success:
                        downloaded += 1
                        # Save checksum for successful download
                        path = Path(result.message.replace("Downloaded: ", ""))
                        for _url, p in tasks:
                            if p.name == path.name:
                                checksums[p.name] = compute_sha256(p)
                                break
                    elif result.message == "":  # 404
                        not_found += 1
                    elif result.message == "skipped":
                        skipped += 1
                    else:
                        failed += 1
                        tqdm.write(f"Failed: {result.message}")

                    # Update progress bars
                    pbar_bytes.update(result.bytes_downloaded)
                    pbar_imgs.update(1)
                    pbar_imgs.set_postfix(
                        ok=downloaded,
                        skip=not_found,
                        fail=failed,
                        refresh=False,
                    )

        return downloaded, failed, not_found, skipped, total_bytes

    async def close(self):
        """Close the connector."""
        await self.connector.close()


async def async_main(args: argparse.Namespace) -> None:
    """Async main function."""
    config = load_config(args.config)

    base_url = config["base_url"]
    start = config["start"]
    end = config["end"]
    output_dir = Path(config.get("output_dir", "images"))
    timeout = config.get("timeout", 30)
    max_retries = config.get("max_retries", 3)
    concurrent = config.get("concurrent_downloads", 10)
    verify_checksums = config.get("verify_checksums", True)

    # Derive filename pattern from URL
    filename_pattern = Path(urlparse(base_url).path).name

    # Checksum file
    checksum_file = output_dir / ".checksums.json"

    # Shutdown event
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("\n\nShutting down gracefully... (press Ctrl+C again to force quit)")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError, NotImplementedError):
            loop.add_signal_handler(sig, signal_handler)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Images {start} to {end}")
    logger.info(f"Output directory: {output_dir.absolute()}")
    if args.verify:
        logger.info("VERIFY MODE - checking existing files only")
    elif args.dry_run:
        logger.info("DRY RUN - no files will be downloaded")
    else:
        logger.info(f"Concurrent downloads: {concurrent}")
        logger.info("Press Ctrl+C to stop gracefully")
    logger.info("-" * 50)

    # Load existing checksums
    checksums = load_checksums(checksum_file) if verify_checksums else {}

    # Prepare download tasks (skip existing files)
    tasks: list[tuple[str, Path]] = []
    existing_files: list[Path] = []

    for i in range(start, end + 1):
        url = base_url.format(i)
        filename = filename_pattern.format(i)
        output_path = output_dir / filename

        if output_path.exists():
            existing_files.append(output_path)
        else:
            tasks.append((url, output_path))

    # Verify-only mode
    if args.verify:
        if not existing_files:
            logger.info("No files to verify.")
            return
        if not checksums:
            logger.info("No checksums found. Run a download first to generate checksums.")
            return

        logger.info(f"Verifying {len(existing_files)} files...")
        with tqdm(total=len(existing_files), unit="file", desc="Verifying") as pbar:
            corrupted = []
            for file_path in existing_files:
                if file_path.name in checksums:
                    actual = compute_sha256(file_path)
                    if actual != checksums[file_path.name]:
                        corrupted.append(file_path)
                        tqdm.write(f"CORRUPTED: {file_path.name}")
                else:
                    tqdm.write(f"NO CHECKSUM: {file_path.name}")
                pbar.update(1)

        logger.info("-" * 50)
        if corrupted:
            logger.info(
                f"Found {len(corrupted)} corrupted files. Run without --verify to re-download."
            )
        else:
            logger.info(f"All {len(existing_files)} files verified OK.")
        return

    # Verify existing files if checksums available (during normal download)
    if verify_checksums and existing_files and checksums:
        logger.info(f"Verifying {len(existing_files)} existing files...")
        corrupted = verify_existing_files(checksums, existing_files)
        if corrupted:
            logger.info(f"Found {len(corrupted)} corrupted files, will re-download")
            for path in corrupted:
                path.unlink()
                # Find the URL for this file and add to tasks
                for i in range(start, end + 1):
                    if filename_pattern.format(i) == path.name:
                        tasks.append((base_url.format(i), path))
                        break

    if not tasks:
        logger.info("All images already downloaded!")
        return

    logger.info(f"Images to download: {len(tasks)}")

    if args.dry_run:
        logger.info("-" * 50)
        logger.info("Files that would be downloaded:")
        for url, path in tasks[:10]:
            logger.info(f"  {url} -> {path.name}")
        if len(tasks) > 10:
            logger.info(f"  ... and {len(tasks) - 10} more")
        logger.info("-" * 50)
        logger.info("Dry run complete. No files downloaded.")
        return

    logger.info("-" * 50)

    downloader = AsyncImageDownloader(timeout, max_retries, concurrent, shutdown_event)

    try:
        with (
            tqdm(
                unit="B", unit_scale=True, unit_divisor=1024, desc="Speed", position=0
            ) as pbar_bytes,
            tqdm(total=len(tasks), unit="img", desc="Images", position=1) as pbar_imgs,
        ):
            downloaded, failed, not_found, skipped, total_bytes = await downloader.download_all(
                tasks, checksums, pbar_bytes, pbar_imgs
            )
    finally:
        await downloader.close()
        # Save checksums
        if verify_checksums and checksums:
            save_checksums(checksum_file, checksums)

    logger.info("-" * 50)

    # Format bytes
    if total_bytes > 1024 * 1024:
        size_str = f"{total_bytes / (1024 * 1024):.1f} MB"
    elif total_bytes > 1024:
        size_str = f"{total_bytes / 1024:.1f} KB"
    else:
        size_str = f"{total_bytes} bytes"

    if shutdown_event.is_set():
        remaining = len(tasks) - (downloaded + failed + not_found + skipped)
        logger.info(
            f"Stopped! Downloaded: {downloaded} ({size_str}), "
            f"Not found: {not_found}, Failed: {failed}, Remaining: {remaining}"
        )
        logger.info("Run again to resume.")
    else:
        logger.info(
            f"Complete! Downloaded: {downloaded} ({size_str}), "
            f"Not found: {not_found}, Failed: {failed}"
        )


def main():
    parser = argparse.ArgumentParser(description="Download numbered images from URLs")
    parser.add_argument(
        "-c", "--config", default="config.json", help="Path to config file (default: config.json)"
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without downloading",
    )
    parser.add_argument(
        "-v", "--verify", action="store_true", help="Verify existing files only, don't download"
    )
    args = parser.parse_args()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
