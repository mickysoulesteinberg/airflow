from airflow.decorators import dag, task_group
import tasks.ingest as ingestion_tasks
from task_groups.load import gcs_to_bigquery
import tasks.utils as helper_tasks
from task_groups.api import fetch_and_prep
from config.datasources import TMDB_DISCOVER_MOVIES, TMDB_CREDITS

from config.logger import get_logger
logger = get_logger(__name__)

YEARS=list(range(2010,2015))
PAGES=[1,2]

@dag()
def top_movie_credits():


    @task_group
    def etl_workflow(config, bigquery_table_name, return_data=None, **kwargs):
        
        initial_setup = ingestion_tasks.setup_etl(**config)
        
        gcs_prefix = initial_setup['gcs_prefix']

        iterate_calls = fetch_and_prep.partial(
            gcs_prefix=gcs_prefix,
            config=config,
            return_data=return_data
        ).expand(**kwargs)


        
        transformed_uris = helper_tasks.reduce_xcoms.override(
            task_id='collect_tmp_uris'
        )(iterate_calls['transformed_uri'])

        gcs_to_bigquery(transformed_uris, config=config, bigquery_table=bigquery_table_name)

        returned_data = {}
        for key in return_data or {}:
            returned_data[key] = helper_tasks.reduce_xcoms.override(
                task_id=f'collect_{key}'
            )(iterate_calls[key])
        
        return returned_data



    movies = etl_workflow(
        config=TMDB_DISCOVER_MOVIES,
        bigquery_table_name='top_movies',
        return_data={'movie_id': 'results[].id'},
        year=YEARS,
        page=PAGES
    )['movie_id']

    etl_workflow(
        config=TMDB_CREDITS,
        bigquery_table_name='top_movies_credits',
        movie=movies
    )



top_movie_credits()
