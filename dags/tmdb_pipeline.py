from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
import tasks.helpers as helper_tasks
import logging, time
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
        'schema': MOVIES_SCHEMA,
        'api_arg_builder': lambda **kwargs: {
            'api_args': {
                'params': {
                    'primary_release_year': kwargs['year'],
                    'sort_by': 'revenue.desc',
                    'page': 1
                }
            },
            'call_params': {'year': kwargs['year']},
            'return_data': {'movie_ids': 'results[].id'}
        },
        # 'return_data': {'movie_ids': 'results[].id'}
    },
    'credits': {
        'api': 'tmdb',
        'api_path': 'movies_credits',
        'staging_table': 'tmdb.credits_stg',
        'final_table': 'tmdb.credits',
        'json_root': ['data'],
        'schema': CREDITS_SCHEMA,
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
        staging_table = create_table(
            dataset_table = dataset_table,
            schema_config = schema_config,
            force_recreate = True,
            retries = 5
        )

        time.sleep(3)
        return staging_table



    
    @task
    def build_api_call(api_arg_builder, **kwargs):
        api_args = api_arg_builder(**kwargs)
        return api_args
    
    @task_group
    def api_ingestion_iterate(
        api,
        api_path,
        staging_table,
        schema_config,
        json_root,
        api_arg_builder = None,
        return_keys = None,
        **api_kwargs):
    
        api_call_dict = None

        # Task: Create dynamic API Fetch task arguments
        if api_arg_builder:
            api_call_dict = build_api_call(api_arg_builder, **api_kwargs)

        # Task: Fetch the data and load to gcs (returns GCS path and any requested data)
        fetched = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_call_dict = api_call_dict
        )

        updated_staging_table = transform_tasks.gcs_to_bq_stg(
            schema_config = schema_config,
            dataset_table = staging_table,
            gcs_path = fetched['gcs_path'],
            json_root = json_root
        )

        result = {key: fetched[key] for key in return_keys}
        result['done'] = updated_staging_table
        return result
    
    @task_group
    def api_ingestion(api_call, return_keys = [], **kwargs):
        api_config = API_CONFIG[api_call]
        api = api_config['api']
        api_path = api_config['api_path']
        schema_config = api_config['schema']['schema']
        merge_cols = api_config['schema']['row_id']
        staging_dataset_table = api_config['staging_table']
        final_table = api_config['final_table']
        api_arg_builder = api_config['api_arg_builder']
        json_root = api_config['json_root']
        
        # Task: Create empty staging table in BigQuery
        staging_table = create_staging_table(
            dataset_table = staging_dataset_table,
            schema_config = schema_config
        )

        # Task group: Loop over kwargs: API Fetch -> GCS -> BQ Staging
        ingestion = api_ingestion_iterate.partial(
            api = api,
            api_path = api_path,
            return_keys = return_keys,
            schema_config = schema_config,
            api_arg_builder = api_arg_builder,
            staging_table = staging_table,
            json_root = json_root
        ).expand(**kwargs)

        merge = loader_tasks.bq_stg_to_final_merge(
            staging_table = staging_table,
            final_table = final_table,
            schema = schema_config,
            merge_cols = merge_cols
        )

        ingestion['done'] >> merge

        returned_data = {}
        for key in return_keys:
            returned_data[key] = helper_tasks.reduce_xcoms.override(
                task_id = f'reduce_xcom_{key}'
            )(ingestion[key])

        return returned_data
    
    movie_ids = api_ingestion.override(
        group_id = 'discover_movies'
    )(
        'discover_movies',
        return_keys = ['movie_ids'],
        year = YEARS
    )['movie_ids']
    
    api_ingestion.override(group_id = 'credits')('credits', movie_id = movie_ids)



tmdb_pipeline()
