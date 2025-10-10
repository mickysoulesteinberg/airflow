from airflow.decorators import dag
from airflow.utils.dates import days_ago
import logging
import tasks.loaders as loader_tasks
from schemas.ssa import NAMES_SCHEMA

logger = logging.getLogger(__name__)

DAG_CONFIG = {
    'names': {
        'final_table': 'ssa.names',
        'staging_table': 'ssa.names_stg',
        'schema': NAMES_SCHEMA
    }
}

@dag(
    start_date=days_ago(1),
    schedule=None,
    catchup=False
)
def ssa_names():

    config = DAG_CONFIG['names']
    bq_schema_config = config['schema']['schema']
    staging_table_name = config['staging_table']

    staging_table = loader_tasks.create_staging_table(
        dataset_table=staging_table_name,
        schema_config=bq_schema_config
    )

ssa_names()