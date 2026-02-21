# TMDB Movie Data Pipeline

TMDB API'sinden film verisi çeken, aylık partisyonlama ve checkpoint desteğiyle çalışan Python pipeline.

## Kurulum

```bash
pip install -r requirements.txt
```

`.env` dosyası oluştur:

```
TMDB_BEARER=<v4_read_access_token>
```

TMDB v4 Bearer token için: [TMDB API Settings](https://www.themoviedb.org/settings/api)

## Kullanım

```bash
python src/movie.py --from 2021-01-01 --to 2023-12-31
```

| Argüman | Varsayılan | Açıklama |
|---|---|---|
| `--from` | `2021-01-01` | Başlangıç tarihi |
| `--to` | `2023-12-31` | Bitiş tarihi |
| `--lang` | `en-US` | Dil kodu |
| `--min-votes` | `0` | Minimum oy sayısı filtresi |
| `--max-pages` | — | Ay başına maksimum sayfa (test için) |
| `--log-level` | `INFO` | Log seviyesi: `DEBUG` `INFO` `WARNING` `ERROR` |
| `--log-file` | — | Log çıktısını dosyaya da yaz |

## Çıktılar

| Dosya | Açıklama |
|---|---|
| `tmdb_monthly_parts/*.parquet` | Aylık ham parça dosyaları |
| `tmdb_movies_<yıllar>.csv` | Birleştirilmiş master CSV (ör. `tmdb_movies_2021-2023.csv`) |
| `tmdb_movies_<yıllar>.parquet` | Birleştirilmiş master Parquet |
| `tmdb_monthly_checkpoint.json` | İlerleme kaydı (resume için) |

Checkpoint sayesinde kesilen çalıştırmalar kaldığı yerden devam eder.

> **Not:** TMDB Discover API sayfa başına 20 sonuç, maksimum 500 sayfa (10.000 film) döner.
> Limit aşıldığında pipeline uyarı logu yazar; daha kısa tarih aralıklarına bölmek bu durumu önler.

## Testler

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

20 unit test — `normalize_to_df`, `month_ranges`, checkpoint (save/load/corrupt) ve `master_paths` fonksiyonlarını kapsar.

## Veri Şeması

| Kolon | Tip | Açıklama |
|---|---|---|
| `tmdb_id` | int | TMDB film ID |
| `title` | str | Film adı |
| `original_title` | str | Orijinal ad |
| `release_date` | str | Yayın tarihi (YYYY-MM-DD) |
| `genres` | str | Türler (pipe-ayrımlı: `Action\|Drama`) |
| `vote_average` | float | Ortalama puan |
| `vote_count` | int | Oy sayısı |
| `popularity` | float | Popülerlik skoru |
| `original_language` | str | Orijinal dil |
| `overview` | str | Film özeti |
| `poster_url` | str | Poster görseli URL |
