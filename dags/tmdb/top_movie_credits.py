from airflow.decorators import dag, task_group
import tasks.ingest as ingestion_tasks
import tasks.transform as transform_tasks
import tasks.load as loader_tasks
import tasks.cleanup as cleanup_tasks
import tasks.utils as helper_tasks
from config.datasources import TMDB_DISCOVER_MOVIES, TMDB_CREDITS

from config.logger import get_logger
logger = get_logger(__name__)


@dag()
def top_movie_credits():

    @task_group
    def fetch_and_prep(api, api_path, 
                         api_arg_builder, arg_fields,
                         gcs_prefix, table_config, data_config,
                         return_data=None,
                         **kwargs):
        call_builder = ingestion_tasks.setup_api_call(
            api_arg_builder=api_arg_builder,
            arg_fields=arg_fields,
            **kwargs
        )

        api_args = call_builder['api_args']
        gcs_file_name = call_builder['gcs_file_name']
        call_metadata = call_builder['metadata']

        fetched = ingestion_tasks.api_fetch_and_load(api=api, api_path=api_path,
                                                     api_args=api_args,
                                                     gcs_prefix=gcs_prefix,
                                                     gcs_file_name=gcs_file_name,
                                                     return_data=return_data,
                                                     metadata=call_metadata)

        result = {key: fetched[key] for key in return_data or {}}

        loaded_gcs_path = fetched['gcs_path']

        transformed_uri = transform_tasks.gcs_transform_for_bq(gcs_path=loaded_gcs_path,
                                                                table_config=table_config,
                                                                data_config=data_config)
        result['transformed_uri'] = transformed_uri
        fetched >> transformed_uri

        return result


    @task_group
    def etl_workflow(config, bigquery_table_name, return_data=None, **kwargs):
        

        initial_setup = ingestion_tasks.setup_etl(**config)
        
        table_config = config['table_config']
        bq_schema_config = table_config['schema']
        bigquery_config = config['bigquery_config']
        bigquery_dataset = bigquery_config['dataset']
        data_config = config.get('data_config')
        api = config['api']
        api_path = config['api_path']
        gcs_prefix = initial_setup['gcs_prefix']
        api_arg_builder = config.get('api_arg_builder')
        arg_fields = config.get('arg_fields')

        iterate_calls = fetch_and_prep.partial(
            api=api, api_path=api_path,
            api_arg_builder=api_arg_builder,
            arg_fields=arg_fields,
            gcs_prefix=gcs_prefix,
            table_config=table_config,
            data_config=data_config,
            return_data=return_data
        ).expand(**kwargs)
        
        transformed_uris = helper_tasks.reduce_xcoms.override(
            task_id='collect_tmp_uris'
        )(iterate_calls['transformed_uri'])

        returned_data = {}
        for key in return_data or {}:
            returned_data[key] = helper_tasks.reduce_xcoms.override(
                task_id=f'collect_{key}'
            )(iterate_calls[key])
        
        created_staging_table = loader_tasks.create_staging_table(
            schema_config=bq_schema_config,
            final_table=bigquery_table_name,
            dataset=bigquery_dataset
        )

        loaded_staging_table = loader_tasks.gcs_to_bq_stg(transformed_uris, created_staging_table)

        merged_final_table = loader_tasks.bq_stg_to_final_merge(
            schema_config=bq_schema_config,
            staging_table=loaded_staging_table,
            final_table=bigquery_table_name,
            dataset=bigquery_dataset,
            table_config=table_config
        )

        cleanup_tasks.delete_bq_staging_table(
            loaded_staging_table,
            wait_for=merged_final_table
        )

        return returned_data



    movies = etl_workflow(
        config=TMDB_DISCOVER_MOVIES,
        bigquery_table_name='top_movies',
        return_data={'movie_id': 'results[].id'},
        year=list(range(2000,2001))
    )['movie_id']

    etl_workflow(
        config=TMDB_CREDITS,
        bigquery_table_name='top_movies_credits',
        movie=movies
    )



top_movie_credits()
