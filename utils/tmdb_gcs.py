from utils.gcs import write_json, write_success_marker

def write_tmdb_movie_json(data: dict, year: int, movie_id: int, dag_id: str):
    path = f"movies/year={year}/movie={movie_id}.json"
    write_json("tmdb", data, path, dag_id)

def write_tmdb_credits_json(data: dict, year: int, movie_id: int, dag_id: str):
    path = f"credits/year={year}/movie={movie_id}.json"
    write_json("tmdb", data, path, dag_id)

def mark_tmdb_movies_success(year: int, dag_id: str):
    write_success_marker("tmdb", "movies", year, dag_id)

def mark_tmdb_credits_success(year: int, dag_id: str):
    write_success_marker("tmdb", "credits", year, dag_id)
