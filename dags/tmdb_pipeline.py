from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
import tasks.helpers as helper_tasks
import logging
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA
from core.bq import make_bq_schema

logger = logging.getLogger(__name__)
YEARS = [2003, 2004]

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
    def ingest_and_load_top_movies_for_year(year):
        # Parameters for this task group
        api_path = 'discover_movies'
        params = {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1}
        call_params = {'year': year}
        schema_config = MOVIES_SCHEMA['schema']
        staging_table = 'tmdb.discover_movies_stg'
        json_path = ['data', 'results']

        # Get the storage info to store data and later retrieve it
        gcs_path = ingestion_tasks.get_storage_data(api, api_path, call_params = call_params)

        # Perform the API Call and save the data to GCS (save the movie IDs for downstream tasks)
        movie_ids = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'params': params},
            gcs_path = gcs_path,
            return_data = {'movie_ids': 'results[].id'}
        )['movie_ids']

        x_insert_movie = transform_tasks.transform_and_insert(schema_config = schema_config, table_id = staging_table, gcs_path = gcs_path, json_path = json_path)

        movie_ids >> x_insert_movie

        return movie_ids

    # -----
    # ----- Get Cast Data for Top Movies and Load to GCS
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
    top_movies = ingest_and_load_top_movies_for_year.expand(year = extract_years_param())
    movie_ids = helper_tasks.collect_xcom_for_expand(top_movies['movie_ids'])
    # Get Xcom for downstream
    # collected = helper_tasks.collect_dicts.expand(dicts_list = top_movies).reduce(flatten_keys = ['movie_ids'])
    # gcs_paths = helper_tasks.extract_field(collected, 'gcs_path')
    # movie_ids = helper_tasks.extract_field(collected, 'movie_ids')

    # test = top_movies.reduce(my_test)

    # Load the raw data from Top movies to BigQuery
    # nothing1 = load_top_movies_to_bigquery.reduce(top_movies['gcs_path'])


    # Load cast data for the movies returned above
    # nothing2 = fetch_cast_for_movie.expand(movie_id = xcom_to_list(top_movies['movie_ids']))
    nothing2 = fetch_cast_for_movie.expand(movie_id = movie_ids)
    return
    

tmdb_pipeline()
