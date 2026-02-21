import os

# TMDB_BEARER olmadan modül import edilemez; test için sahte değer atıyoruz.
# Gerçek API çağrısı yapan testler ayrıca mock'lanmalıdır.
os.environ.setdefault("TMDB_BEARER", "test_token_for_testing")
