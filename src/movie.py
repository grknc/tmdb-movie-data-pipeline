#!/usr/bin/env python3
# movie_scrapping.py
# Robust TMDB scraping + monthly backfill + checkpointing + output CSV/Parquet
# Usage (example):
#   python movie_scrapping.py --from 2021-01-01 --to 2025-12-31

import os
import sys
import time
import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

# ---------------- Robust .env loader ----------------
def load_project_dotenv():
    """
    Load .env preferring the script directory, then cwd, then find_dotenv fallback.
    Works from terminal, pycharm run/debug, and interactive consoles.
    """
    cand_paths = []
    # script directory if available
    if "__file__" in globals():
        cand_paths.append(Path(__file__).resolve().parent / ".env")
    # sys.argv[0] fallback
    argv0 = sys.argv[0] if len(sys.argv) > 0 else ""
    if argv0:
        p = Path(argv0).resolve()
        if p.is_file():
            cand_paths.append(p.parent / ".env")
    # current working dir
    cand_paths.append(Path.cwd() / ".env")

    # unique preserve order
    seen = set()
    cand = []
    for c in cand_paths:
        s = str(c)
        if s not in seen:
            seen.add(s)
            cand.append(c)

    # debug print (safe)
    # print("dotenv candidates:", cand)

    for c in cand:
        if c.exists():
            load_dotenv(dotenv_path=str(c), override=False)
            print("Loaded .env from:", c)
            return c

    # fallback
    ff = find_dotenv(raise_error_if_not_found=False)
    if ff:
        load_dotenv(ff)
        print("Loaded .env via find_dotenv:", ff)
        return Path(ff)

    print("No .env found via candidates.")
    return None

# call loader
load_project_dotenv()

# ---------------- Config from env or defaults ----------------
TMDB_BEARER = os.getenv("TMDB_BEARER")
if not TMDB_BEARER:
    raise RuntimeError("Set TMDB_BEARER env var first (v4 read access token).")

BASE = "https://api.themoviedb.org/3"
HEADERS = {"Authorization": f"Bearer {TMDB_BEARER}", "Accept": "application/json"}

# default output / checkpoint locations
OUT_DIR = Path("tmdb_monthly_parts")
OUT_DIR.mkdir(exist_ok=True)
MASTER_CSV = Path("tmdb_movies_full25.csv")
MASTER_PARQUET = Path("tmdb_movies_full25.parquet")
CHECKPOINT_MONTHS = Path("tmdb_monthly_checkpoint.json")

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ---------- HTTP helper with retries ----------
def safe_get(path: str, params: Optional[Dict[str,Any]] = None,
             max_retries: int = 6, backoff_base: float = 1.0) -> Dict[str,Any]:
    url = f"{BASE}{path}"
    params = params or {}
    for attempt in range(max_retries):
        try:
            resp = SESSION.get(url, params=params, timeout=25)
        except requests.RequestException as e:
            wait = backoff_base * (2 ** attempt)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:  # rate limited
            ra = resp.headers.get("Retry-After")
            try:
                wait = int(ra) if ra else backoff_base * (2 ** attempt)
            except Exception:
                wait = backoff_base * (2 ** attempt)
            print(f"Rate limited. Waiting {wait}s (attempt {attempt+1})")
            time.sleep(wait)
            continue
        if 500 <= resp.status_code < 600:
            wait = backoff_base * (2 ** attempt)
            print(f"Server error {resp.status_code}. Waiting {wait}s and retrying.")
            time.sleep(wait)
            continue

        # other client errors -> bubble up with message
        raise RuntimeError(f"HTTP {resp.status_code} - {resp.text}")
    raise RuntimeError(f"Max retries exceeded for {url}")

# ---------- TMDB small helpers ----------
def get_image_base_and_size() -> Tuple[str,str]:
    cfg = safe_get("/configuration")
    imgs = cfg.get("images", {})
    base = imgs.get("secure_base_url") or imgs.get("base_url")
    sizes = imgs.get("poster_sizes", [])
    size = "w500" if "w500" in sizes else (sizes[len(sizes)//2] if sizes else "original")
    return base, size

def get_genre_map(lang: str = "en-US") -> Dict[int,str]:
    j = safe_get("/genre/movie/list", params={"language": lang})
    return {g["id"]: g["name"] for g in j.get("genres", [])}

# ---------- Discover (paged) ----------
def discover_all(date_from: str, date_to: str, language: str = "en-US",
                 min_votes: int = 0, max_pages: Optional[int] = None,
                 polite_sleep: float = 0.08) -> List[Dict[str,Any]]:
    params = {
        "primary_release_date.gte": date_from,
        "primary_release_date.lte": date_to,
        "language": language,
        "vote_count.gte": min_votes,
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "page": 1
    }

    first = safe_get("/discover/movie", params=params)
    total_pages = first.get("total_pages", 1)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    movies: List[Dict[str,Any]] = []
    # page 1
    movies.extend(first.get("results", []))
    # pages 2..N
    if total_pages >= 2:
        for p in tqdm(range(2, total_pages + 1), desc="discover pages"):
            params["page"] = p
            j = safe_get("/discover/movie", params=params)
            movies.extend(j.get("results", []))
            time.sleep(polite_sleep)
    return movies

# ---------- normalize ----------
def normalize_to_df(raw_movies: List[Dict[str,Any]], image_base: str, poster_size: str, genres_map: Dict[int,str]) -> pd.DataFrame:
    rows = []
    for m in raw_movies:
        genre_names = [genres_map.get(gid, str(gid)) for gid in m.get("genre_ids", [])]
        poster_path = m.get("poster_path")
        poster_url = f"{image_base}{poster_size}{poster_path}" if poster_path else None
        rows.append({
            "tmdb_id": m.get("id"),
            "title": m.get("title"),
            "original_title": m.get("original_title"),
            "release_date": m.get("release_date"),
            "genres": "|".join(genre_names),
            "vote_average": m.get("vote_average"),
            "vote_count": m.get("vote_count"),
            "popularity": m.get("popularity"),
            "original_language": m.get("original_language"),
            "overview": m.get("overview"),
            "poster_url": poster_url
        })
    df = pd.DataFrame(rows)
    cols = ["tmdb_id","title","original_title","release_date","genres","vote_average","vote_count","popularity","original_language","overview","poster_url"]
    df = df.reindex(columns=cols)
    return df

# ---------- checkpoint helpers (monthly) ----------
def save_checkpoint(d: Dict[str,Any]):
    tmp = Path(str(CHECKPOINT_MONTHS) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    tmp.replace(CHECKPOINT_MONTHS)  # atomic on same filesystem

def load_checkpoint() -> Dict[str,Any]:
    if CHECKPOINT_MONTHS.exists():
        try:
            with open(CHECKPOINT_MONTHS, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            print("Warning: checkpoint file corrupted, starting fresh.")
            return {"done_months": []}
    return {"done_months": []}

def part_filename(start: str, end: str) -> Path:
    safe_start = start.replace(":", "-")
    safe_end = end.replace(":", "-")
    return OUT_DIR / f"tmdb_{safe_start}_to_{safe_end}.parquet"

# ---------- date partitioning ----------
def month_ranges(start_date_str: str, end_date_str: str) -> List[Tuple[str,str]]:
    s = pd.to_datetime(start_date_str).date()
    e = pd.to_datetime(end_date_str).date()
    cur = date(s.year, s.month, 1)
    out = []
    while cur <= e:
        nxt = cur + relativedelta(months=1)
        last_day = (nxt - timedelta(days=1))
        out.append((cur.isoformat(), min(last_day, e).isoformat()))
        cur = nxt
    return out

# ---------- monthly backfill runner ----------
def run_monthly_backfill(start: str, end: str, language: str = "en-US", min_votes: int = 0, max_pages: Optional[int] = None):
    print("Fetching TMDB configuration and genres...")
    image_base, poster_size = get_image_base_and_size()
    genres_map = get_genre_map(language)
    print("Image base:", image_base, "size:", poster_size)
    print("Genre sample:", list(genres_map.items())[:6])

    ranges = month_ranges(start, end)
    cp = load_checkpoint()
    done = set(cp.get("done_months", []))

    for a,b in ranges:
        key = f"{a}_{b}"
        if key in done:
            print(f"SKIP {a} -> {b} (already done)")
            continue

        print(f"\n=== Processing {a} -> {b} ===")
        try:
            raw = discover_all(a, b, language=language, min_votes=min_votes, max_pages=max_pages)
            print("Raw items fetched:", len(raw))
            if not raw:
                df_part = pd.DataFrame(columns=["tmdb_id","title","original_title","release_date","genres","vote_average","vote_count","popularity","original_language","overview","poster_url"])
            else:
                df_part = normalize_to_df(raw, image_base, poster_size, genres_map)
                df_part = df_part.drop_duplicates(subset=["tmdb_id"]).reset_index(drop=True)

            outp = part_filename(a,b)
            df_part.to_parquet(outp, index=False)
            print("Saved part:", outp, "rows:", len(df_part))

            # update checkpoint
            done.add(key)
            save_checkpoint({"done_months": sorted(list(done))})

        except Exception as e:
            print("ERROR during month:", a,b)
            print(e)
            raise

    # concat parts to master
    print("\nConcatenating monthly parts...")
    parts = sorted(OUT_DIR.glob("tmdb_*.parquet"))
    if not parts:
        print("No parts found to concatenate. Exiting.")
        return
    dfs = [pd.read_parquet(p) for p in parts]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["tmdb_id"]).reset_index(drop=True)
    print("Final unique rows:", len(df))
    df.to_csv(MASTER_CSV, index=False, encoding="utf-8")
    df.to_parquet(MASTER_PARQUET, index=False)
    print("Saved master files:", MASTER_CSV, MASTER_PARQUET)

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="TMDB monthly backfill scraper")
    p.add_argument("--from", dest="date_from", required=False, default=os.getenv("DATE_FROM", "2021-01-01"), help="Start date YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=False, default=os.getenv("DATE_TO", "2023-12-31"), help="End date YYYY-MM-DD")
    p.add_argument("--lang", dest="lang", required=False, default=os.getenv("LANGUAGE", "en-US"), help="language code")
    p.add_argument("--min-votes", dest="min_votes", type=int, default=0, help="minimum vote_count")
    p.add_argument("--max-pages", dest="max_pages", type=int, default=None, help="limit discover pages per month (optional)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    print("Starting backfill:", args.date_from, "->", args.date_to, "lang:", args.lang)
    run_monthly_backfill(args.date_from, args.date_to, language=args.lang, min_votes=args.min_votes, max_pages=args.max_pages)
