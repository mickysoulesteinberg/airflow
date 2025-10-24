from airflow.decorators import dag
from airflow.utils.dates import days_ago
import tasks.load as loader_tasks
import tasks.transform as transform_tasks
import tasks.cleanup as cleanup_tasks
from config.datasources import SSA_NAMES

from config.logger import get_logger
logger = get_logger(__name__)

@dag()
def ssa_names():

    config = SSA_NAMES

    table_config = config['table_config']
    bq_schema_config = table_config['schema']
    bigquery_config = config['bigquery_config']
    bigquery_dataset = bigquery_config['dataset']
    bigquery_table_name = bigquery_config['table']
    data_config = config['data_config']
    storage_config = config['storage_config']
    gcs_bucket = storage_config['gcs_bucket']
    gcs_path = storage_config['gcs_path']

    created_staging_table = loader_tasks.create_staging_table(
        schema_config=bq_schema_config,
        final_table = bigquery_table_name,
        dataset=bigquery_dataset
    )

    transformed_uris = transform_tasks.gcs_transform_for_bq(
        storage_config=storage_config,
        table_config=table_config,
        data_config=data_config
    )

    loaded_staging_table = loader_tasks.gcs_to_bq_stg(
        transformed_uris,
        created_staging_table
    )

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

    
ssa_names()