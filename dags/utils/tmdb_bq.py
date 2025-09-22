from utils.bq import insert_rows

def insert_movies(movies: list[dict]):
    insert_rows('tmdb', 'movies', movies)

def insert_credits(credits: list[dict]):
    insert_rows('tmdb', 'credits', credits)

# def insert_movies(movies: list[dict]):
#     table_id = f"{PROJECT}.{DATASET}.movies"
#     errors = client.insert_rows_json(table_id, movies)
#     if errors:
#         raise RuntimeError(f"[BQ] Insert errors: {errors}")
#     print(f"[BQ] Inserted {len(movies)} rows into {table_id}")

# def insert_credits(credits: list[dict]):
#     table_id = f"{PROJECT}.{DATASET}.credits"
#     errors = client.insert_rows_json(table_id, credits)
#     if errors:
#         raise RuntimeError(f"[BQ] Insert errors: {errors}")
#     print(f"[BQ] Inserted {len(credits)} rows into {table_id}")
