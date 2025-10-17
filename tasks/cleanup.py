from airflow.decorators import task
from core.gcs import delete_gcs_prefix
from pipeline.cleanup import delete_gcs_files
from core.logger import get_logger
logger = get_logger(__name__)


@task
def delete_bq_staging_table(staging_table, wait_for=None):
    '''
    Deletes a BigQuery staging table. 
    Use wait_for to ensure this task runs after other tasks.
    '''
    from core.bq import delete_table
    delete_table(staging_table)
    return