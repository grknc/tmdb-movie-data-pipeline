# TMDB Movie Data Pipeline

TMDB API'sinden film verisi çeken, aylık partisyonlama ve checkpoint desteğiyle çalışan Python pipeline.

## Gereksinimler

```bash
pip install -r requirements.txt
```

## Kurulum

1. `.env` dosyası oluştur:

```
TMDB_BEARER=<v4_read_access_token>
```

TMDB v4 Bearer token için: [TMDB API Settings](https://www.themoviedb.org/settings/api)

2. Pipeline'ı çalıştır:

```bash
python src/movie.py --from 2021-01-01 --to 2023-12-31
```

## Kullanım

```
python src/movie.py [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--lang dil] [--min-votes N] [--max-pages N]
```

| Argüman | Varsayılan | Açıklama |
|---|---|---|
| `--from` | `2021-01-01` | Başlangıç tarihi |
| `--to` | `2023-12-31` | Bitiş tarihi |
| `--lang` | `en-US` | Dil kodu |
| `--min-votes` | `0` | Minimum oy sayısı filtresi |
| `--max-pages` | — | Ay başına maksimum sayfa (test için) |

## Çıktılar

| Dosya | Açıklama |
|---|---|
| `tmdb_monthly_parts/*.parquet` | Aylık ham parça dosyaları |
| `tmdb_movies_full25.csv` | Birleştirilmiş master CSV |
| `tmdb_movies_full25.parquet` | Birleştirilmiş master Parquet |
| `tmdb_monthly_checkpoint.json` | İlerleme kaydı (resume için) |

Checkpoint sayesinde kesilen çalıştırmalar kaldığı yerden devam eder.

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
