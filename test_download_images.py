#!/usr/bin/env python3
"""Tests for the async image downloader."""

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import aiohttp
import pytest
from aioresponses import aioresponses

from download_images import (
    AsyncImageDownloader,
    DownloadResult,
    compute_sha256,
    load_checksums,
    load_config,
    save_checksums,
    validate_format_string,
    verify_existing_files,
)


class TestValidateFormatString:
    """Tests for format string validation."""

    def test_valid_format_with_padding(self):
        """Valid format string with zero-padding should not raise."""
        validate_format_string("https://example.com/img_{:04d}.jpg", 1)

    def test_valid_format_simple(self):
        """Valid format string without padding should not raise."""
        validate_format_string("https://example.com/img_{}.jpg", 1)

    def test_invalid_format_no_placeholder(self):
        """URL without placeholder should exit."""
        with pytest.raises(SystemExit):
            validate_format_string("https://example.com/img.jpg", 1)

    def test_invalid_format_wrong_type(self):
        """URL with string placeholder should exit when given int."""
        with pytest.raises(SystemExit):
            validate_format_string("https://example.com/img_{name}.jpg", 1)


class TestLoadConfig:
    """Tests for config loading."""

    def test_load_valid_config(self, tmp_path: Path):
        """Valid config file should load successfully."""
        config_file = tmp_path / "config.json"
        config_data = {
            "base_url": "https://example.com/img_{:04d}.jpg",
            "start": 1,
            "end": 100,
        }
        config_file.write_text(json.dumps(config_data))

        config = load_config(str(config_file))

        assert config["base_url"] == config_data["base_url"]
        assert config["start"] == 1
        assert config["end"] == 100

    def test_load_config_file_not_found(self):
        """Missing config file should exit."""
        with pytest.raises(SystemExit) as exc_info:
            load_config("/nonexistent/config.json")
        assert "not found" in str(exc_info.value)

    def test_load_config_invalid_json(self, tmp_path: Path):
        """Invalid JSON should exit."""
        config_file = tmp_path / "config.json"
        config_file.write_text("not valid json")

        with pytest.raises(SystemExit) as exc_info:
            load_config(str(config_file))
        assert "Invalid JSON" in str(exc_info.value)

    def test_load_config_missing_required_keys(self, tmp_path: Path):
        """Missing required keys should exit."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"base_url": "https://example.com/{}.jpg"}))

        with pytest.raises(SystemExit) as exc_info:
            load_config(str(config_file))
        assert "Missing required" in str(exc_info.value)

    def test_load_config_no_format_placeholder(self, tmp_path: Path):
        """URL without format placeholder should exit."""
        config_file = tmp_path / "config.json"
        config_data = {
            "base_url": "https://example.com/img.jpg",
            "start": 1,
            "end": 100,
        }
        config_file.write_text(json.dumps(config_data))

        with pytest.raises(SystemExit) as exc_info:
            load_config(str(config_file))
        assert "format placeholder" in str(exc_info.value)


class TestChecksums:
    """Tests for checksum functionality."""

    def test_compute_sha256(self, tmp_path: Path):
        """SHA256 should be computed correctly."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b"hello world")

        result = compute_sha256(test_file)

        # Known SHA256 of "hello world"
        assert result == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    def test_save_and_load_checksums(self, tmp_path: Path):
        """Checksums should be saved and loaded correctly."""
        checksum_file = tmp_path / ".checksums.json"
        checksums = {"file1.jpg": "abc123", "file2.jpg": "def456"}

        save_checksums(checksum_file, checksums)
        loaded = load_checksums(checksum_file)

        assert loaded == checksums

    def test_load_checksums_missing_file(self, tmp_path: Path):
        """Missing checksum file should return empty dict."""
        checksum_file = tmp_path / ".checksums.json"

        result = load_checksums(checksum_file)

        assert result == {}

    def test_verify_existing_files_detects_corruption(self, tmp_path: Path):
        """Corrupted files should be detected."""
        # Create a file
        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"original content")
        original_hash = compute_sha256(test_file)

        # Corrupt the file
        test_file.write_bytes(b"corrupted content")

        checksums = {"test.jpg": original_hash}
        corrupted = verify_existing_files(checksums, [test_file])

        assert test_file in corrupted

    def test_verify_existing_files_valid(self, tmp_path: Path):
        """Valid files should not be flagged as corrupted."""
        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"content")
        file_hash = compute_sha256(test_file)

        checksums = {"test.jpg": file_hash}
        corrupted = verify_existing_files(checksums, [test_file])

        assert corrupted == []


class TestAsyncImageDownloader:
    """Tests for the async image downloader."""

    @pytest.mark.asyncio
    async def test_download_success(self, tmp_path: Path):
        """Successful download should save file and return success."""
        url = "https://example.com/img_0001.jpg"
        output_path = tmp_path / "img_0001.jpg"
        shutdown_event = asyncio.Event()

        downloader = AsyncImageDownloader(
            timeout=30, max_retries=3, concurrent=1, shutdown_event=shutdown_event
        )

        with aioresponses() as m:
            m.get(url, body=b"fake image content")

            import aiohttp

            async with aiohttp.ClientSession() as session:
                result = await downloader.download(session, url, output_path)

        await downloader.close()

        assert result.success is True
        assert result.url == url
        assert result.bytes_downloaded == len(b"fake image content")
        assert output_path.exists()
        assert output_path.read_bytes() == b"fake image content"

    @pytest.mark.asyncio
    async def test_download_404(self, tmp_path: Path):
        """404 response should return failure with empty message."""
        url = "https://example.com/img_0001.jpg"
        output_path = tmp_path / "img_0001.jpg"
        shutdown_event = asyncio.Event()

        downloader = AsyncImageDownloader(
            timeout=30, max_retries=3, concurrent=1, shutdown_event=shutdown_event
        )

        with aioresponses() as m:
            m.get(url, status=404)

            import aiohttp

            async with aiohttp.ClientSession() as session:
                result = await downloader.download(session, url, output_path)

        await downloader.close()

        assert result.success is False
        assert result.message == ""
        assert not output_path.exists()

    @pytest.mark.asyncio
    async def test_download_shutdown(self, tmp_path: Path):
        """Download should be skipped when shutdown is set."""
        url = "https://example.com/img_0001.jpg"
        output_path = tmp_path / "img_0001.jpg"
        shutdown_event = asyncio.Event()
        shutdown_event.set()  # Set shutdown before download

        downloader = AsyncImageDownloader(
            timeout=30, max_retries=3, concurrent=1, shutdown_event=shutdown_event
        )

        async with aiohttp.ClientSession() as session:
            result = await downloader.download(session, url, output_path)

        await downloader.close()

        assert result.success is False
        assert result.message == "skipped"

    @pytest.mark.asyncio
    async def test_download_all_multiple_files(self, tmp_path: Path):
        """download_all should download multiple files concurrently."""
        shutdown_event = asyncio.Event()
        downloader = AsyncImageDownloader(
            timeout=30, max_retries=3, concurrent=2, shutdown_event=shutdown_event
        )

        tasks = [
            ("https://example.com/img_0001.jpg", tmp_path / "img_0001.jpg"),
            ("https://example.com/img_0002.jpg", tmp_path / "img_0002.jpg"),
        ]
        checksums: dict[str, str] = {}

        # Mock progress bars
        pbar_bytes = MagicMock()
        pbar_imgs = MagicMock()

        with aioresponses() as m:
            m.get("https://example.com/img_0001.jpg", body=b"content1")
            m.get("https://example.com/img_0002.jpg", body=b"content2")

            downloaded, failed, not_found, skipped, total_bytes = await downloader.download_all(
                tasks, checksums, pbar_bytes, pbar_imgs
            )

        await downloader.close()

        assert downloaded == 2
        assert failed == 0
        assert not_found == 0
        assert skipped == 0
        assert total_bytes == len(b"content1") + len(b"content2")
        assert (tmp_path / "img_0001.jpg").exists()
        assert (tmp_path / "img_0002.jpg").exists()
        assert len(checksums) == 2

    @pytest.mark.asyncio
    async def test_download_all_graceful_shutdown(self, tmp_path: Path):
        """download_all should handle graceful shutdown without warnings."""
        shutdown_event = asyncio.Event()
        downloader = AsyncImageDownloader(
            timeout=30, max_retries=3, concurrent=1, shutdown_event=shutdown_event
        )

        tasks = [
            ("https://example.com/img_0001.jpg", tmp_path / "img_0001.jpg"),
        ]
        checksums: dict[str, str] = {}

        pbar_bytes = MagicMock()
        pbar_imgs = MagicMock()

        # Set shutdown immediately
        shutdown_event.set()

        downloaded, _failed, _not_found, _skipped, _total_bytes = await downloader.download_all(
            tasks, checksums, pbar_bytes, pbar_imgs
        )

        await downloader.close()

        # Should exit cleanly without downloading
        assert downloaded == 0


class TestDownloadResult:
    """Tests for DownloadResult class."""

    def test_download_result_creation(self):
        """DownloadResult should store all fields."""
        result = DownloadResult("https://example.com/img.jpg", True, "Downloaded", 1024)

        assert result.url == "https://example.com/img.jpg"
        assert result.success is True
        assert result.message == "Downloaded"
        assert result.bytes_downloaded == 1024

    def test_download_result_default_bytes(self):
        """DownloadResult should default bytes_downloaded to 0."""
        result = DownloadResult("https://example.com/img.jpg", False, "Error")

        assert result.bytes_downloaded == 0


class TestFilenameDerivation:
    """Tests for filename derivation from URL."""

    def test_derive_filename_from_url(self):
        """Filename should be derived from URL path."""
        from urllib.parse import urlparse

        urls_and_expected = [
            ("https://example.com/image_{:04d}.jpg", "image_0001.jpg"),
            ("https://cdn.site.com/photos/pic_{}.png", "pic_1.png"),
            ("https://storage.example.com/bucket/img{:03d}.webp", "img001.webp"),
        ]

        for url, expected in urls_and_expected:
            pattern = Path(urlparse(url).path).name
            filename = pattern.format(1)
            assert filename == expected, f"URL: {url}"
