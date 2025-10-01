from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
from itertools import chain
import logging

logger = logging.getLogger(__name__)
YEARS = [2003]

@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params = {'years': YEARS}
)
def tmdb_pipeline():
    api = 'tmdb'

    @task
    def extract_years_param():
        context = get_current_context()
        return context['params']['years']

    @task_group
    def ingest_top_movies_for_year(year):
        api_path = 'discover_movies'
        params = {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1}

        # Get the storage info to store data and later retrieve it
        gcs_uri = ingestion_tasks.get_storage_data(api, api_path, call_params = {'year': year})

        # Performa the API Call and save the data to GCS
        movie_ids = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'params': params},
            gcs_uri = gcs_uri,
            return_data = {'movie_ids': 'results[].id'}
        )['movie_ids']

        return {'gcs_uri': gcs_uri, 'movie_ids': movie_ids}

    # Task group to load top movies to BigQuery
    @task
    def load_top_movies_to_bigquery(gcs_uris):
        logger.warning('Inserting Top movies to Bigquery (task in progress)')
        return gcs_uris
    
    @task
    def concat_list(iterable_to_chain):
        return list(chain.from_iterable(iterable_to_chain))

    # Task group to get cast for top movies and load to GCS
    @task_group
    def fetch_cast_for_movie(movie_id = 122):
        api_path = 'movies_credits'
        path_vars = {'movie_id': movie_id}

        gcs_uri = ingestion_tasks.get_storage_data(api, api_path, call_params = path_vars)

        data = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'path_vars': path_vars},
            gcs_uri = gcs_uri,
            return_data = {
                'top_cast': 'cast[0:3].name',
                'actor_roles': 'cast[0:5].[name, character]',
                'actor_ids': 'cast[].id',
                'directors': 'crew[?job=="Director"].name',
                'most_popular': 'max_by(cast[], &popularity).name'
            }
        )

        return {'gcs_uri': gcs_uri, 'data': data}

    # Load the movies to GCS and return movie IDs and GCS URIS for downstream tasks
    top_movies = ingest_top_movies_for_year.expand(year = extract_years_param())
    
    # Load the raw data from Top movies to BigQuery
    nothing1 = load_top_movies_to_bigquery(top_movies['gcs_uri'])

    # Load cast data for the movies returned above
    nothing2 = fetch_cast_for_movie.expand(movie_id = concat_list(top_movies['movie_ids']))
    return
    

tmdb_pipeline()
