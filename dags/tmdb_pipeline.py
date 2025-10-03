from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
import tasks.helpers as helper_tasks
import logging
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA

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
        final_table = 'tmdb.discover_movies'
        json_root = ['data', 'results']

        # Perform the API Call and save the data to GCS (save the movie IDs for downstream tasks)
        top_movies = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'params': params},
            call_params = call_params,
            return_data = {'movie_ids': 'results[].id'}
        )

        gcs_path = top_movies['gcs_path']
        movie_ids = top_movies['movie_ids']

        # Transform the raw JSON and insert to BQ staging table
        x_insert_movie = transform_tasks.transform_and_insert(
            schema_config = schema_config,
            table_id = staging_table,
            gcs_path = gcs_path,
            json_root = json_root
        )

        # Create task to merge movies
        movies_merge_factory = loader_tasks.bq_stage_merge(task_id = 'movies_merge', schema = schema_config, merge_cols = ['id'])

        # Load the staging data into the BQ final table
        x_merge_movie = movies_merge_factory(
            staging_table = staging_table,
            final_table = final_table
        )
        # Load GCS -> BQ after ingesting the data and saving to GCS
        x_insert_movie >> x_merge_movie

        return movie_ids

    # -----
    # ----- Get Cast Data for Top Movies and Load to GCS
    @task_group
    def ingest_and_load_cast_for_movie(movie_id):
        api_path = 'movies_credits'
        path_vars = {'movie_id': movie_id}
        call_params = {'movie': movie_id}
        schema_config = CREDITS_SCHEMA['schema']
        staging_table = 'tmdb.credits_stg'
        final_table = 'tmdb.credits',
        json_root = ['data']

        gcs_path = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = {'path_vars': path_vars},
            call_params = call_params
        )['gcs_path']

        # Transform the raw JSON and insert to BQ staging table
        x_insert_cast = transform_tasks.transform_and_insert(
            schema_config = schema_config,
            table_id = staging_table,
            gcs_path = gcs_path,
            json_root = json_root
        )

        return


    # ----- Test entire pipeline
    # -----

    # Load the movies to GCS and return movie IDs and GCS URIS for downstream tasks
    top_movies = ingest_and_load_top_movies_for_year.expand(year = extract_years_param())
    movie_ids = helper_tasks.collect_xcom_for_expand(top_movies['movie_ids'])

    nothing = ingest_and_load_cast_for_movie.expand(movie_id = movie_ids)

    # ----- Only test Credits
    # nothing = ingest_and_load_cast_for_movie.expand(movie_id = [122, 12])
    return
    

tmdb_pipeline()
