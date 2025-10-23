from airflow.decorators import dag, task_group, task
from airflow.operators.python import get_current_context
import tasks.ingest as ingestion_tasks
import tasks.transform as transform_tasks
import tasks.load as loader_tasks
import tasks.cleanup as cleanup_tasks
from config.datasources import TMDB_DISCOVER_MOVIES, TMDB_CREDITS

from config.logger import get_logger
logger = get_logger(__name__)


@dag()
def top_movie_credits():
    api_config = TMDB_DISCOVER_MOVIES

    @task_group
    def setup(config):
        path_setup = ingestion_tasks.setup_api_path(config=config)
        bq_setup = transform_tasks.setup_for_bq(config)

        return {
            'api': path_setup['api'],
            'api_path': path_setup['api_path'],
            'gcs_prefix': path_setup['gcs_prefix'],
            'table_config': bq_setup['table_config'],
            'schema_config': bq_setup['schema_config'],
            'bigquery_dataset': bq_setup['dataset'],
            'api_root': bq_setup['api_root'],
            # 'source_type': bq_setup['source_type'],
            # 'merge_cols': bq_setup['merge_cols']
        }
    
    
    @task_group
    def api_call_iterate(config, api, api_path, gcs_prefix, table_config, api_root, **kwargs):
        call_builder = ingestion_tasks.setup_api_call(config=config, **kwargs)

        api_args = call_builder['api_args']
        gcs_file_name = call_builder['gcs_file_name']
        call_metadata = call_builder['metadata']

        fetched = ingestion_tasks.api_fetch_and_load(api=api, api_path=api_path,
                                                     api_args=api_args,
                                                     gcs_prefix=gcs_prefix,
                                                     gcs_file_name=gcs_file_name,
                                                    #  return_data=return_data,
                                                     metadata=call_metadata)

        loaded_gcs_path = fetched['gcs_path']

        transformed_uri = transform_tasks.gcs_transform_for_bq(loaded_gcs_path,
                                                                table_config=table_config,
                                                                api_root=api_root)

        fetched >> transformed_uri

        return transformed_uri


    @task_group
    def etl_workflow(config, bigquery_table_name, **kwargs):
        initial_setup = setup(config)

        table_config = initial_setup['table_config']
        bq_schema_config = initial_setup['schema_config']
        bigquery_dataset = initial_setup['bigquery_dataset']
        api_root = initial_setup['api_root']
        api = initial_setup['api']
        api_path = initial_setup['api_path']
        gcs_prefix = initial_setup['gcs_prefix']

        iterate_calls = api_call_iterate(config, api=api, api_path=api_path,
                                         gcs_prefix=gcs_prefix,
                                         table_config=table_config, api_root=api_root,
                                         **kwargs)
        
        transformed_uris = iterate_calls
        
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


    movies = etl_workflow(TMDB_DISCOVER_MOVIES, 'top_grossing_movies', year=2020)
    

top_movie_credits()
