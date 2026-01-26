# Numbered Image Downloader

A fast, concurrent Python script for downloading numbered images from URLs.

## Features

- Concurrent downloads with configurable parallelism
- Resumes interrupted downloads (skips existing files)
- Graceful shutdown with Ctrl+C (no partial files)
- Atomic writes using temp files
- Connection pooling for performance
- Exponential backoff on retries

## Installation

Requires Python 3.10+

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install requests
```

## Usage

1. Copy the example config:
   ```bash
   cp config.example.json config.json
   ```

2. Edit `config.json` with your URL pattern:
   ```json
   {
       "base_url": "https://example.com/image_{:04d}.jpg",
       "start": 1,
       "end": 100,
       "output_dir": "images",
       "timeout": 30,
       "max_retries": 3,
       "concurrent_downloads": 20
   }
   ```

3. Run:
   ```bash
   uv run download
   # or
   python download_images.py

   # Use a different config file:
   uv run download -c my_config.json
   ```

## Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `base_url` | URL pattern with format placeholder (e.g., `{:04d}`) | (required) |
| `start` | Starting number | (required) |
| `end` | Ending number | (required) |
| `output_dir` | Directory to save images | `images` |
| `timeout` | Request timeout in seconds | `30` |
| `max_retries` | Retry count for failed downloads | `3` |
| `concurrent_downloads` | Number of parallel downloads | `10` |

Output filenames are derived from the URL pattern automatically.

## Graceful Shutdown

Press `Ctrl+C` once to stop gracefully (finishes current downloads).
Press `Ctrl+C` twice to force quit immediately.

Run again to resume - already downloaded files are skipped.
