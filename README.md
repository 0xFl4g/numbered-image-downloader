# Numbered Image Downloader

A fast, async Python tool for downloading numbered images from URLs.

## Features

- **Async I/O** - Uses aiohttp for efficient concurrent downloads
- **Live speed** - Dual progress bars showing MB/s and images/sec
- **Checksum verification** - SHA256 checksums detect corrupted files
- **Verify mode** - Check existing files without downloading
- **Dry run mode** - Preview downloads without fetching
- **Resume support** - Skips existing files, re-downloads corrupted ones
- **Graceful shutdown** - Ctrl+C stops cleanly (no partial files)
- **Atomic writes** - Temp files prevent incomplete downloads

## Installation

Requires Python 3.10+

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install aiohttp tqdm
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
       "concurrent_downloads": 20,
       "verify_checksums": true
   }
   ```

3. Run:
   ```bash
   uv run download

   # Dry run (preview without downloading):
   uv run download --dry-run

   # Verify existing files only:
   uv run download --verify

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
| `verify_checksums` | Verify existing files against saved checksums | `true` |

Output filenames are derived from the URL pattern automatically.

## Checksum Verification

Downloaded files are checksummed (SHA256) and stored in `.checksums.json`. On subsequent runs:
- Existing files are verified against stored checksums
- Corrupted files are automatically re-downloaded
- New checksums are saved after successful downloads

Use `--verify` to check existing files without downloading:
```bash
uv run download --verify
```

## Graceful Shutdown

Press `Ctrl+C` once to stop gracefully (finishes current downloads).
Press `Ctrl+C` twice to force quit immediately.

Run again to resume - already downloaded files are skipped.

## Testing

```bash
uv sync --all-extras
uv run pytest
```
