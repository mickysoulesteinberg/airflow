from airflow.decorators import dag
from airflow.utils.dates import days_ago
from config.logger import get_logger
import tasks.load as loader_tasks
import tasks.transform as transform_tasks
import tasks.cleanup as cleanup_tasks
from schemas.ssa import NAMES_SCHEMA

logger = get_logger(__name__)

DAG_CONFIG = {
    'names': {
        'final_table': 'ssa.names',
        'staging_table': 'ssa.names_stg',
        'table_config': NAMES_SCHEMA,
        'gcs_bucket': 'ssa_data_bucket',
        'gcs_path': 'names/yob188*.txt',
        'gcs_tmp_dir': 'names/tmp/'
    }
}

@dag(
    start_date=days_ago(1),
    schedule=None,
    catchup=False
)
def ssa_names():

    config = DAG_CONFIG['names']
    table_config = config['table_config']
    bq_schema_config = table_config['schema']
    staging_table_name = config['staging_table']
    gcs_bucket = config.get('gcs_bucket')
    gcs_path = config.get('gcs_path')
    final_table_name = config['final_table']
    merge_cols = table_config['merge_cols']
    gcs_tmp_dir = config['gcs_tmp_dir']

    created_staging_table = loader_tasks.create_staging_table_og(
        dataset_table=staging_table_name,
        schema_config=bq_schema_config
    )

    transformed_uris = transform_tasks.gcs_transform_for_bigquery_og(gcs_path, table_config, source_bucket=gcs_bucket, new_dir=gcs_tmp_dir)

    loaded_staging_table = loader_tasks.gcs_to_bq_stg(transformed_uris, created_staging_table)

    merged_final_table = loader_tasks.bq_stg_to_final_merge_og(
        staging_table=loaded_staging_table,
        final_table=final_table_name,
        schema=bq_schema_config,
        merge_cols=merge_cols
    )

    
ssa_names()