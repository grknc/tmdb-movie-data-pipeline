"""
Unit tests for src/movie.py

Sadece saf fonksiyonlar test edilir (API çağrısı yapılmaz).
conftest.py TMDB_BEARER'ı import öncesi set eder.
"""
import json
import pytest
import pandas as pd

import src.movie as movie

COLS = movie.COLS


# ------------------------------------------------------------------ #
#  normalize_to_df                                                     #
# ------------------------------------------------------------------ #
class TestNormalizeToDf:
    def _raw(self, **overrides):
        base = {
            "id": 1,
            "title": "Test Movie",
            "original_title": "Test Movie OT",
            "release_date": "2023-06-15",
            "genre_ids": [28, 35],
            "vote_average": 7.5,
            "vote_count": 1000,
            "popularity": 120.5,
            "original_language": "en",
            "overview": "A test movie.",
            "poster_path": "/abc123.jpg",
        }
        base.update(overrides)
        return base

    def test_output_columns_match_schema(self):
        df = movie.normalize_to_df(
            [self._raw()], "https://cdn/", "w500", {28: "Action", 35: "Comedy"}
        )
        assert list(df.columns) == COLS

    def test_field_values(self):
        df = movie.normalize_to_df(
            [self._raw()], "https://cdn/", "w500", {28: "Action", 35: "Comedy"}
        )
        row = df.iloc[0]
        assert row["tmdb_id"] == 1
        assert row["title"] == "Test Movie"
        assert row["genres"] == "Action|Comedy"
        assert row["poster_url"] == "https://cdn/w500/abc123.jpg"
        assert row["vote_average"] == 7.5

    def test_missing_poster_returns_none(self):
        df = movie.normalize_to_df(
            [self._raw(poster_path=None)], "https://cdn/", "w500", {}
        )
        assert df.iloc[0]["poster_url"] is None

    def test_unknown_genre_id_falls_back_to_string(self):
        df = movie.normalize_to_df(
            [self._raw(genre_ids=[9999])], "https://cdn/", "w500", {}
        )
        assert df.iloc[0]["genres"] == "9999"

    def test_empty_genre_list(self):
        df = movie.normalize_to_df(
            [self._raw(genre_ids=[])], "https://cdn/", "w500", {28: "Action"}
        )
        assert df.iloc[0]["genres"] == ""

    def test_empty_input_returns_empty_df_with_schema(self):
        df = movie.normalize_to_df([], "https://cdn/", "w500", {})
        assert len(df) == 0
        assert list(df.columns) == COLS

    def test_multiple_rows(self):
        raws = [self._raw(id=i, title=f"Movie {i}") for i in range(1, 4)]
        df = movie.normalize_to_df(raws, "https://cdn/", "w500", {28: "Action", 35: "Comedy"})
        assert len(df) == 3
        assert list(df["tmdb_id"]) == [1, 2, 3]


# ------------------------------------------------------------------ #
#  month_ranges                                                        #
# ------------------------------------------------------------------ #
class TestMonthRanges:
    def test_single_full_month(self):
        assert movie.month_ranges("2023-01-01", "2023-01-31") == [
            ("2023-01-01", "2023-01-31")
        ]

    def test_two_full_months(self):
        assert movie.month_ranges("2023-01-01", "2023-02-28") == [
            ("2023-01-01", "2023-01-31"),
            ("2023-02-01", "2023-02-28"),
        ]

    def test_end_clipped_mid_month(self):
        result = movie.month_ranges("2023-01-01", "2023-01-15")
        assert result == [("2023-01-01", "2023-01-15")]

    def test_start_day_ignored_always_first_of_month(self):
        # mid-month start → range begins from 1st of that month
        result = movie.month_ranges("2023-03-15", "2023-04-30")
        assert result[0][0] == "2023-03-01"

    def test_year_boundary(self):
        result = movie.month_ranges("2022-12-01", "2023-01-31")
        assert result == [
            ("2022-12-01", "2022-12-31"),
            ("2023-01-01", "2023-01-31"),
        ]

    def test_same_start_and_end(self):
        result = movie.month_ranges("2023-06-10", "2023-06-10")
        assert result == [("2023-06-01", "2023-06-10")]


# ------------------------------------------------------------------ #
#  checkpoint                                                          #
# ------------------------------------------------------------------ #
class TestCheckpoint:
    def test_load_returns_empty_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(movie, "CHECKPOINT_MONTHS", tmp_path / "cp.json")
        assert movie.load_checkpoint() == {"done_months": []}

    def test_load_returns_empty_when_file_corrupted(self, tmp_path, monkeypatch):
        cp = tmp_path / "cp.json"
        cp.write_text("not valid json {{ }", encoding="utf-8")
        monkeypatch.setattr(movie, "CHECKPOINT_MONTHS", cp)
        assert movie.load_checkpoint() == {"done_months": []}

    def test_save_and_load_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(movie, "CHECKPOINT_MONTHS", tmp_path / "cp.json")
        data = {"done_months": ["2023-01-01_2023-01-31", "2023-02-01_2023-02-28"]}
        movie.save_checkpoint(data)
        assert movie.load_checkpoint() == data

    def test_save_leaves_no_tmp_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(movie, "CHECKPOINT_MONTHS", tmp_path / "cp.json")
        movie.save_checkpoint({"done_months": []})
        assert not (tmp_path / "cp.json.tmp").exists()

    def test_save_overwrites_previous(self, tmp_path, monkeypatch):
        monkeypatch.setattr(movie, "CHECKPOINT_MONTHS", tmp_path / "cp.json")
        movie.save_checkpoint({"done_months": ["2023-01-01_2023-01-31"]})
        movie.save_checkpoint({"done_months": ["2023-01-01_2023-01-31", "2023-02-01_2023-02-28"]})
        result = movie.load_checkpoint()
        assert len(result["done_months"]) == 2


# ------------------------------------------------------------------ #
#  master_paths                                                        #
# ------------------------------------------------------------------ #
class TestMasterPaths:
    def test_different_years(self):
        csv, parquet = movie.master_paths("2021-01-01", "2023-12-31")
        assert csv.name == "tmdb_movies_2021-2023.csv"
        assert parquet.name == "tmdb_movies_2021-2023.parquet"

    def test_same_year(self):
        csv, parquet = movie.master_paths("2024-01-01", "2024-12-31")
        assert csv.name == "tmdb_movies_2024.csv"
        assert parquet.name == "tmdb_movies_2024.parquet"
