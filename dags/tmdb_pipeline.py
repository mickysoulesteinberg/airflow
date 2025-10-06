from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
import tasks.helpers as helper_tasks
import logging
from core.bq import create_table
from utils.helpers import get_valid_kwargs
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA

logger = logging.getLogger(__name__)

# ----- DAG PARAMS
# -----
YEARS = [2003, 2004]

# ----- Dag-level constants (static)
# -----
API_CONFIG = {
    'discover_movies': {
        'api': 'tmdb',
        'api_path': 'discover_movies',
        'staging_table': 'tmdb.discover_movies_stg',
        'final_table': 'tmdb.discover_movies',
        'json_root': ['data', 'results'],
        'schema': MOVIES_SCHEMA['schema'],
        'api_arg_builder': lambda **kwargs: {
            'api_args': {
                'params': {
                    'primary_release_year': kwargs['year'],
                    'sort_by': 'revenue.desc',
                    'page': 1
                }
            },
            'call_params': {'year': kwargs['year']},
            # 'return_data': {'movie_ids': 'results[].id'}
        },
        'return_data': {'movie_ids': 'results[].id'}
    },
    'credits': {
        'api': 'tmdb',
        'api_path': 'movies_credits',
        'staging_table': 'tmdb.credits_stg',
        'final_table': 'tmdb.credits',
        'json_root': ['data'],
        'schema': CREDITS_SCHEMA['schema'],
        'api_arg_builder': lambda **kwargs: {
            'api_args': {'path_vars': {'movie_id': kwargs['movie_id']}},
            'call_params': {'movie': kwargs['movie_id']}
        }
    }
}


@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params = {'years': YEARS},
    user_defined_macros = {'API_CONFIG': API_CONFIG}
)
def tmdb_pipeline():
   
    @task
    def create_staging_table(dataset_table, schema_config):
        create_table(
            dataset_table = dataset_table,
            schema_config = schema_config,
            force_recreate = True
        )
        return dataset_table
    
    @task
    def build_api_call(api_arg_builder, **kwargs):
        api_args = api_arg_builder(**kwargs)
        return api_args
    
    @task_group
    def api_ingestion_iterate(api, api_path, staging_table, api_arg_builder, **kwargs):
    
        # Task: Create dynamic API Fetch task arguments
        api_call_dict = build_api_call(api_arg_builder, **kwargs)

        # Task: Fetch the data and load to gcs (returns GCS path and any requested data)
        fetched = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_call_dict = api_call_dict
        )

        gcs_path = fetched['gcs_path']

        # Load to staging table using staging_table

        return fetched
    
    @task_group
    def api_ingestion(api_call, **kwargs):
        api_config = API_CONFIG[api_call]
        api = api_config['api']
        api_path = api_config['api_path']
        schema_config = api_config['schema']
        staging_dataset_table = api_config['staging_table']
        api_arg_builder = api_config['api_arg_builder']
        
        # Task: Create empty staging table in BigQuery
        staging_table = create_staging_table(dataset_table = staging_dataset_table, schema_config = schema_config)

        # Task group: Loop over kwargs: API Fetch -> GCS -> BQ Staging
        x_returned = api_ingestion_iterate.partial(
            api = api,
            api_path = api_path,
            api_arg_builder = api_arg_builder,
            staging_table = staging_table
        ).expand(**kwargs)

        returned_data = helper_tasks.reduce_xcoms(x_returned['movie_ids'])

        return returned_data
    
    returned_data = api_ingestion.override(group_id = 'discover_movies')('discover_movies', year = YEARS)
    
    @task
    def my_task(returned_data):
        return returned_data
    
    returned_data2 = my_task(returned_data)

    movie_ids = helper_tasks.reduce_xcoms.override(task_id = 'collect_movie_ids')(returned_data['movie_ids'])

    api_ingestion.override(group_id = 'credits')('credits', movie_id = movie_ids)
    # api_ingestion.override(
    #     group_id = 'credits'
    # ).partial(
    #     api_call = 'credits'
    # ).expand(movie_id = movie_ids)

    return returned_data2

tmdb_pipeline()