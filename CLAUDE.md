# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A single-script Python pipeline that fetches movie data from the TMDB API using the `/discover/movie` endpoint, partitioned month-by-month over a configurable date range. Outputs both CSV and Parquet formats with checkpointing so interrupted runs can resume.

## Running the Script

```bash
python movie.py --from 2021-01-01 --to 2025-12-31
python movie.py --from 2021-01-01 --to 2025-12-31 --lang en-US --min-votes 10 --max-pages 5
```

CLI arguments (all optional with defaults):
- `--from` / `--to`: Date range (YYYY-MM-DD). Defaults to `DATE_FROM`/`DATE_TO` env vars, then `2021-01-01`/`2023-12-31`.
- `--lang`: Language code (default: `en-US`, or `LANGUAGE` env var).
- `--min-votes`: Filter by minimum vote count (default: 0).
- `--max-pages`: Cap discover pages per month, useful for testing.

## Configuration

Requires a `.env` file in the project root (or script directory) with:

```
TMDB_BEARER=<your_v4_read_access_token>
```

Optional env vars: `DATE_FROM`, `DATE_TO`, `LANGUAGE`.

## Dependencies

Install with pip:

```bash
pip install requests pandas tqdm python-dotenv python-dateutil pyarrow
```

## Output Files

| File | Description |
|------|-------------|
| `tmdb_monthly_parts/tmdb_<start>_to_<end>.parquet` | Per-month raw parts |
| `tmdb_movies_full25.csv` | Master deduplicated CSV |
| `tmdb_movies_full25.parquet` | Master deduplicated Parquet |
| `tmdb_monthly_checkpoint.json` | Tracks completed months for resumption |

## Architecture

All logic lives in `movie.py` as a flat script with these layers:

1. **`.env` loader** (`load_project_dotenv`) — searches script dir, argv[0] dir, and cwd before falling back to `find_dotenv`.
2. **HTTP layer** (`safe_get`) — retries up to 6 times with exponential backoff; handles 429 rate-limit via `Retry-After` header and 5xx server errors.
3. **TMDB helpers** — `get_image_base_and_size()` fetches CDN config; `get_genre_map()` builds an `id → name` dict.
4. **Discovery** (`discover_all`) — pages through `/discover/movie` for a given date window, writing small page-level checkpoints as it goes.
5. **Normalization** (`normalize_to_df`) — flattens raw results into a fixed-column DataFrame; genres stored as pipe-delimited string; poster URL constructed from CDN base.
6. **Monthly backfill** (`run_monthly_backfill`) — splits the full date range into calendar months, skips already-completed months from `tmdb_monthly_checkpoint.json`, saves each month as a Parquet part, then concatenates all parts into master files at the end.
7. **CLI** (`parse_args`) — argparse wrapper; falls back to env vars for defaults.
