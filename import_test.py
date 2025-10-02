# import_test.py
print("🔎 Testing imports...")

try:
    from tasks.ingestion import api_fetch
    print("✅ tasks.ingestion import works")
except Exception as e:
    print("❌ tasks.ingestion failed:", e)

try:
    from apis.tmdb import discover_movies
    print("✅ apis.tmdb import works")
except Exception as e:
    print("❌ apis.tmdb failed:", e)

try:
    from core.gcs import upload_to_gcs
    print("✅ core.gcs import works")
except Exception as e:
    print("❌ core.gcs failed:", e)

try:
    from schemas.tmdb import MOVIES_STAGE
    print("✅ schemas.tmdb import works")
except Exception as e:
    print("❌ schemas.tmdb failed:", e)

try:
    from transforms.helpers import transform_record
    print("✅ transforms.json_to_bq import works")
except Exception as e:
    print("❌ transforms.json_to_bq failed:", e)
