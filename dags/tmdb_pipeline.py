from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
from itertools import chain
import logging
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA

logger = logging.getLogger(__name__)
YEARS = [2003]

# -----
# ----- Set up BigQuery Loaders
movies_merge_factory = loader_tasks.bq_stage_merge(schema = MOVIES_SCHEMA, merge_cols = ['id'])

@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params = {'years': YEARS}
)
def tmdb_pipeline():
    api = 'tmdb'


    # -----
    # ----- Discover Movies by Year --> GCS
    @task
    def extract_years_param():
        context = get_current_context()
        return context['params']['years']

    @task_group
    def ingest_top_movies_for_year(year):
        api_path = 'discover_movies'
        params = {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1}

        # Get the storage info to store data and later retrieve it
        gcs_path = ingestion_tasks.get_storage_data(api, api_path, call_params = {'year': year})

        # Performa the API Call and save the data to GCS
        movie_ids = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'params': params},
            gcs_path = gcs_path,
            return_data = {'movie_ids': 'results[].id'}
        )['movie_ids']

        return {'gcs_path': gcs_path, 'movie_ids': movie_ids}


    # -----
    # ----- Load Top Movie Data from GCS to BigQuery ----- #

    @task_group
    def load_top_movies_to_bigquery(gcs_paths):

        insert_movie = transform_tasks.transform_and_insert.override(
            task_id = 'insert_movie'
        ).partial(
            schema_config = MOVIES_SCHEMA,
            table_id = 'tmdb.discover_movies_stg'
        ).expand(
            gcs_path = gcs_paths
        )

        return insert_movie




    





    # -----
    # ----- Get Cast Data for Top Movies and Load to GCS

    @task
    def concat_list(iterable_to_chain):
        return list(chain.from_iterable(iterable_to_chain))

    @task_group
    def fetch_cast_for_movie(movie_id):
        api_path = 'movies_credits'
        path_vars = {'movie_id': movie_id}
        call_params = {'movie': movie_id}

        gcs_path = ingestion_tasks.get_storage_data(api, api_path, call_params = call_params)

        data = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'path_vars': path_vars},
            gcs_path = gcs_path,
            return_data = {
                'top_cast': 'cast[0:3].name',
                'actor_roles': 'cast[0:5].[name, character]',
                'actor_ids': 'cast[].id',
                'directors': 'crew[?job=="Director"].name',
                'most_popular': 'max_by(cast[], &popularity).name'
            }
        )

        return {'gcs_path': gcs_path, 'data': data}

    # Load the movies to GCS and return movie IDs and GCS URIS for downstream tasks
    top_movies = ingest_top_movies_for_year.expand(year = extract_years_param())
    
    # Load the raw data from Top movies to BigQuery
    nothing1 = load_top_movies_to_bigquery(top_movies['gcs_path'])

    # Load cast data for the movies returned above
    # nothing2 = fetch_cast_for_movie.expand(movie_id = concat_list(top_movies['movie_ids']))
    return
    

tmdb_pipeline()
