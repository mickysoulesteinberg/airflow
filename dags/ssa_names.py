from airflow.decorators import dag
from airflow.utils.dates import days_ago
import logging
import tasks.loaders as loader_tasks
import tasks.transforms as transform_tasks
from schemas.ssa import NAMES_SCHEMA

logger = logging.getLogger(__name__)

DAG_CONFIG = {
    'names': {
        'final_table': 'ssa.names',
        'staging_table': 'ssa.names_stg',
        'table_config': NAMES_SCHEMA,
        'gcs_bucket': 'ssa_data_bucket',
        'gcs_path': 'names/yob188*.txt'
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

    staging_table = loader_tasks.create_staging_table(
        dataset_table=staging_table_name,
        schema_config=bq_schema_config
    )

    transformed_uris = transform_tasks.gcs_transform_for_bigquery(gcs_path, table_config, bucket_name=gcs_bucket)

ssa_names()