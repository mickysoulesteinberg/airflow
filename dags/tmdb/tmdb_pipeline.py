from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from pipeline.dag_helpers import make_gcs_path_factory
import tasks.ingest as ingestion_tasks
import tasks.load as loader_tasks
import tasks.utils as helper_tasks
import tasks.transform as transform_tasks
import tasks.cleanup as cleanup_tasks
from config.logger import get_logger
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA

logger = get_logger(__name__)

# ----- DAG PARAMS
# -----
YEARS = [2003]

# ----- Dag-level constants (static)
# -----
API_CONFIG = {
    'discover_movies': {
        'api': 'tmdb',
        'api_path': 'discover_movies',
        'staging_table': 'tmdb.discover_movies_stg',
        'final_table': 'tmdb.discover_movies',
        'json_root': ['data', 'results'],
        'table_config': MOVIES_SCHEMA,
        'api_arg_builder': lambda **kwargs: {
            'api_args': {
                'params': {
                    'primary_release_year': kwargs['year'],
                    'sort_by': 'revenue.desc',
                    'page': 1
                }
            },
            'call_id': f'year{kwargs['year']}',
            'return_data': {'movie_ids': 'results[].id'}
        }
    },
    'credits': {
        'api': 'tmdb',
        'api_path': 'movies_credits',
        'staging_table': 'tmdb.credits_stg',
        'final_table': 'tmdb.credits',
        'json_root': ['data'],
        'table_config': CREDITS_SCHEMA,
        'api_arg_builder': lambda **kwargs: {
            'api_args': {'path_vars': {'movie_id': kwargs['movie_id']}},
            'call_id': f'movie{kwargs['movie_id']}'
        }
    }
}


@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params={'years': YEARS},
    user_defined_macros={
        'API_CONFIG': API_CONFIG,

    }
)
def tmdb_pipeline():
    
    @task(multiple_outputs=True)
    def setup_api_call(api_arg_builder, **kwargs):
        api_call_dict = api_arg_builder(**kwargs)
        context = get_current_context()
        _, make_gcs_file_name = make_gcs_path_factory(context)
        gcs_file_name = make_gcs_file_name(api_call_dict['call_id'])
        api_args = api_call_dict['api_args']
        return_data = api_call_dict.get('return_data')
        return {'gcs_file_name': gcs_file_name, 'api_args': api_args, 'return_data': return_data}


    @task_group
    def api_ingestion_iterate(api, api_path, table_config, json_root, gcs_folders,
                              api_arg_builder=None, return_keys=None, **api_kwargs):
    
        gcs_prefix = gcs_folders['gcs_prefix']

        task_builder = setup_api_call(api_arg_builder, **api_kwargs)
        api_args = task_builder['api_args']
        gcs_file_name = task_builder['gcs_file_name']
        return_data = task_builder['return_data']

        fetched = ingestion_tasks.api_fetch_and_load_og(api=api, api_path=api_path, api_args=api_args, 
                                                     gcs_prefix=gcs_prefix, gcs_file_name=gcs_file_name,
                                                     return_data=return_data)
        loaded_gcs_path = fetched['gcs_path']

        transformed_uris = transform_tasks.gcs_transform_for_bigquery_og(loaded_gcs_path,
                                                                      table_config,
                                                                      json_root=json_root)

        fetched >> transformed_uris

        result = {key: fetched[key] for key in return_keys}
        result['gcs_uri'] = transformed_uris
        return result
    

    @task(multiple_outputs=True)
    def create_gcs_prefixes(api, api_path):
        context = get_current_context()

        make_gcs_prefix, _ = make_gcs_path_factory(context)
        gcs_prefix = make_gcs_prefix(api, api_path)
        gcs_tmp_prefix = f'{gcs_prefix}/tmp'
        return {'gcs_prefix': gcs_prefix, 'gcs_tmp_prefix': gcs_tmp_prefix}


    @task_group
    def api_ingestion(api_call, return_keys=[], **kwargs):

        api_config = API_CONFIG[api_call]
        api = api_config['api']
        api_path = api_config['api_path']
        staging_table = api_config['staging_table']
        final_table = api_config['final_table']

        gcs_folders = create_gcs_prefixes(api, api_path)
        gcs_temp_prefix = gcs_folders['gcs_tmp_prefix']

        table_config = api_config['table_config']
        bq_schema_config = table_config['schema']

        # Task: Create empty staging table in BigQuery
        created_staging_table = loader_tasks.create_staging_table_og(
            dataset_table=staging_table,
            schema_config=bq_schema_config
        )

        # Task group: Loop over kwargs: API Fetch -> GCS -> BQ Staging
        ingestion = api_ingestion_iterate.partial(
            api=api_config['api'],
            api_path=api_config['api_path'],
            return_keys=return_keys,
            table_config=table_config,
            api_arg_builder=api_config['api_arg_builder'],
            json_root=api_config['json_root'],
            gcs_folders=gcs_folders
        ).expand(**kwargs)



        transformed_uris = helper_tasks.reduce_xcoms.override(
            task_id='collect_temp_uris'
        )(ingestion['gcs_uri'])


        loaded_staging_table = loader_tasks.gcs_to_bq_stg(transformed_uris, created_staging_table)

        merged_final_table = loader_tasks.bq_stg_to_final_merge_og(
            staging_table=loaded_staging_table,
            final_table=final_table,
            schema=bq_schema_config,
            merge_cols=table_config['row_id']
        )

        cleanup_tasks.delete_bq_staging_table(loaded_staging_table, wait_for=merged_final_table)

        returned_data = {}
        for key in return_keys:
            returned_data[key] = helper_tasks.reduce_xcoms.override(
                task_id=f'collect_{key}'
            )(ingestion[key])

        return returned_data
    
    movie_ids = api_ingestion.override(
        group_id='discover_movies'
    )(
        'discover_movies',
        return_keys=['movie_ids'],
        year=YEARS
    )['movie_ids']
    
    api_ingestion.override(
        group_id='credits'
    )('credits', movie_id=movie_ids)



tmdb_pipeline()
