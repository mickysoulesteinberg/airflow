from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import logging

from apis.tmdb import discover_movies_by_year, get_movie_credits
# from apis._scratch_tmdb_gcs import (
#     write_tmdb_movie_json,
#     write_tmdb_credits_json,
#     mark_tmdb_movies_success,
#     mark_tmdb_credits_success
# )
# from apis._scratch_tmdb_bq import insert_tmdb_movies, insert_tmdb_credits
from core._scratch_status import get_next_row, create_status_table, seed_status_table, update_status

logger = logging.getLogger(__name__)

# Parameters
PAGE_LIMIT = 5  # max pages to fetch per year
DAG_ID = 'tmdb_backfill_top_credits'

@dag(
    start_date = days_ago(1),
    schedule_interval = None,
    catchup = False,
    tags = ["tmdb", "backfill"]
)
def tmdb_backfill_top_credits_dag():
    
    @task
    def setup_status():
        years = list(range(2000, 2025))

        # Movies status table
        create_status_table('tmdb', DAG_ID, 'fetch_movies', id_col='year')
        seed_status_table('tmdb', DAG_ID, 'fetch_movies', years, id_col = 'year')

        # Credits status table
        create_status_table('tmdb', DAG_ID, 'fetch_credits', id_col='year')
        seed_status_table('tmdb', DAG_ID, 'fetch_credits', years, id_col ='year')
    
    @task
    def pick_year(dag_run = None):
        override = dag_run.conf.get('year') if dag_run else None
        if override:
            return int(override)
        row = get_next_row('tmdb', DAG_ID, 'fetch_movies', id_col='year')
        if not row:
            logger.info('[DAG] No more years to process, exiting.')
            return None
        logger.info(f'[DAG] Picked year {row['year']} for processing.')
        return row['year']

    @task
    def fetch_movies(year: int = 2000):
        update_status('tmdb', DAG_ID, 'fetch_movies', year, 'in_progress', id_col='year')

        results = discover_movies_by_year(year, sort_by='popularity', sort_order='desc', page=1)
        movie_ids = []
        movie_rows = []

        for movie in results['results']:
            movie_ids.append(movie['id'])
            movie_rows.append({
                'movie_id': movie['id'],
                'title': movie['title'],
                'release_date': movie.get('release_date'),
                'revenue': movie.get('revenue'),
                'popularity': movie.get('popularity'),
                'raw_json': movie
            })
            write_tmdb_movie_json(movie, year, movie['id'], DAG_ID)
        
        insert_tmdb_movies(movie_rows)
        mark_tmdb_movies_success(year, DAG_ID)

        logger.info(f'[DAG] Completed movies for {year} with {len(movie_ids)} records.')
        return movie_ids

    @task
    def fetch_cast(year: int, movie_ids: list[int]):
        credit_rows = []

        for movie_id in movie_ids:
            credits = get_movie_credits(movie_id)
            write_tmdb_credits_json(credits, year, movie_id, DAG_ID)

            for person in credits.get('cast', []):
                credit_rows.append({
                    'movie_id': movie_id,
                    'person_id': person['id'],
                    'name': person['name'],
                    'character': person.get('character'),
                    'credit_type': 'cast',
                    'raw_json': person
                })
        
        insert_tmdb_credits(credit_rows)
        mark_tmdb_credits_success(year, DAG_ID)
        update_status('tmdb', DAG_ID, 'fetch_credits', year, 'completed', id_col = 'year')

        logger.info(f'[DAG] Completed credits for {year} with {len(credit_rows)} records.')
    
    # DAG workflow
    setup = setup_status()
    year = pick_year()
    if year:
        movies = fetch_movies(year)
        fetch_cast(year, movies)

dag = tmdb_backfill_top_credits_dag()