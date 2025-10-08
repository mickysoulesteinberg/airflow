from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
import tasks.loaders as loader_tasks
import tasks.helpers as helper_tasks
import logging, time, json
from core.bq import create_table, load_all_gcs_to_bq
from core.gcs import gcs_transform_and_store, delete_gcs_files
from schemas.tmdb import MOVIES_SCHEMA, CREDITS_SCHEMA
from dag_helpers.paths import make_gcs_path_factory

logger = logging.getLogger(__name__)

# ----- DAG PARAMS
# -----
YEARS = [2003, 2004]

# ----- Dag-level constants (static)
# -----
API_CONFIG1 = {
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
        'schema': CREDITS_SCHEMA,
        'api_arg_builder': lambda **kwargs: {
            'api_args': {'path_vars': {'movie_id': kwargs['movie_id']}},
            'call_params': {'movie': kwargs['movie_id']},
            'call_id': f'movie{kwargs['movie_id']}'
        }
    }
}


@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params = {'years': YEARS},
    user_defined_macros = {
        'API_CONFIG': API_CONFIG1,

    }
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

    @task(multiple_outputs = True)
    def gcs_initial_transform(schema_config, gcs_path, json_root = None):
        tmp_gcs = gcs_transform_and_store(schema_config, gcs_path, json_root = json_root)
        return {'gcs_path': tmp_gcs['tmp_path'], 'gcs_uri': tmp_gcs['tmp_uri']}
    
    @task
    def build_api_call(api_arg_builder, **kwargs):
        api_call_dict = api_arg_builder(**kwargs)
        return api_call_dict
    
    @task(multiple_outputs = True)
    def setup_api_call(api_arg_builder, gcs_prefix, **kwargs):
        api_call_dict = api_arg_builder(**kwargs)
        context = get_current_context()
        _, make_gcs_file_name = make_gcs_path_factory(context)
        gcs_path = f'{gcs_prefix}/{make_gcs_file_name(api_call_dict['call_id'])}'
        api_args = api_call_dict['api_args']
        return_data = api_call_dict['return_data']
        return {'gcs_path': gcs_path, 'api_args': api_args, 'return_data': return_data}

    
    @task
    def gcs_to_stg(gcs_uris, dataset_table):
        return load_all_gcs_to_bq(gcs_uris, dataset_table)
    
    @task
    def cleanup_temp_files(gcs_paths):
        delete_gcs_files(gcs_paths)
        return

    @task_group
    def api_ingestion_iterate(
        api,
        api_path,
        schema_config,
        json_root,
        gcs_prefix,
        api_arg_builder = None,
        return_keys = None,
        **api_kwargs):
    
        # api_call_dict = None
        # gcs_path = None
        # gcs_file_name = None

        # # TODO merge this task and below
        # # Task: Create dynamic API Fetch task arguments
        # if api_arg_builder:
        #     api_call_dict = build_api_call(api_arg_builder, **api_kwargs)

        # # TODO Handle case where inputs are None
        # gcs_path = f'{gcs_prefix}/{make_gcs_file_name(api_call_dict['call_id'])}'

        task_builder = setup_api_call(api_arg_builder, gcs_prefix, **api_kwargs)
        api_args = task_builder['api_args']
        gcs_path = task_builder['gcs_path']
        return_data = task_builder['return_data']

        # Task: Fetch the data and load to gcs (returns GCS path and any requested data)
        # fetched = ingestion_tasks.api_fetch(
        #     api = api,
        #     api_path = api_path,
        #     gcs_path = gcs_path,
        #     api_call_dict = api_call_dict
        # )

        fetched = ingestion_tasks.api_fetch_and_load(
            api=api, api_path = api_path, api_args = api_args, gcs_path = gcs_path,
            return_data = return_data
        )

        transformed = gcs_initial_transform(
            schema_config,
            fetched['gcs_path'],
            json_root
        )

        result = {key: fetched[key] for key in return_keys}
        result['gcs_uri'] = transformed['gcs_uri']
        result['gcs_path'] = transformed['gcs_path']
        return result
    

    @task(multiple_outputs = True)
    def access_api_call_params(api_call):
        context = get_current_context()

        api_config = API_CONFIG1[api_call]

        make_gcs_prefix, make_gcs_file_name = make_gcs_path_factory(context)
        api = api_config['api']
        api_path = api_config['api_path']
        gcs_prefix = make_gcs_prefix(api, api_path)

        return {
            'api': api,
            'api_path': api_path,
            'schema_config': api_config['schema']['schema'],
            'merge_cols': api_config['schema']['row_id'],
            'staging_dataset_table': api_config['staging_table'],
            'final_table': api_config['final_table'],
            'api_arg_builder': api_config['api_arg_builder'],
            'json_root': api_config['json_root'],
            'gcs_prefix': gcs_prefix,
            'make_gcs_file_name': make_gcs_file_name
        }

    @task
    def create_gcs_folder_path(api, api_path):
        context = get_current_context()

        make_gcs_prefix, _ = make_gcs_path_factory(context)
        gcs_prefix = make_gcs_prefix(api, api_path)
        return gcs_prefix

    @task_group
    def api_ingestion(api_call, return_keys = [], **kwargs):

        api_config = API_CONFIG1[api_call]
        api = api_config['api']
        api_path = api_config['api_path']

        gcs_prefix = create_gcs_folder_path(api, api_path)
        # make_gcs_file_name = gcs_path_data['make_gcs_file_name']
        # Task: Get Params
        # task_group_params = access_api_call_params(api_call)

        bq_schema_config = api_config['schema']['schema']

        # Task: Create empty staging table in BigQuery
        staging_table = create_staging_table(
            dataset_table = api_config['staging_table'],
            schema_config = bq_schema_config
        )

        # Task group: Loop over kwargs: API Fetch -> GCS -> BQ Staging
        ingestion = api_ingestion_iterate.partial(
            api = api_config['api'],
            api_path = api_config['api_path'],
            return_keys = return_keys,
            schema_config = bq_schema_config,
            api_arg_builder = api_config['api_arg_builder'],
            staging_table = staging_table,
            json_root = api_config['json_root'],
            gcs_prefix = gcs_prefix
            # make_gcs_file_name = make_gcs_file_name
        ).expand(**kwargs)



        gcs_uris = helper_tasks.reduce_xcoms.override(
            task_id = 'reduce_xcom_gcs_uri'
        )(ingestion['gcs_uri'])


        stage = gcs_to_stg(gcs_uris, staging_table)

        merge = loader_tasks.bq_stg_to_final_merge(
            staging_table = staging_table,
            final_table = api_config['final_table'],
            schema = bq_schema_config,
            merge_cols = api_config['schema']['row_id']
        )

        gcs_paths = helper_tasks.reduce_xcoms.override(
            task_id = 'reduce_xcom_gcs_path'
        )(ingestion['gcs_path'])

        cleanup = cleanup_temp_files(gcs_paths)
        # ingestion['done'] >> merge

        stage >> merge >> cleanup

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
    
    # api_ingestion.override(group_id = 'credits')('credits', movie_id = movie_ids)



tmdb_pipeline()
