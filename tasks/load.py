from airflow.decorators import task
from core.bq import load_all_gcs_to_bq
from config.logger import get_logger
import pipeline.load as pipeline


logger = get_logger(__name__)


@task
def bq_stg_to_final_merge(staging_table, config=None, **overrides):
    return pipeline.bq_stg_to_final_merge(staging_table, config=config, **overrides)

@task
def create_staging_table(config=None, **overrides):
    '''Creates a blank staging table to load data to. Requires confirmation of table creation before returning.'''
    return pipeline.create_staging_table(config=config, **overrides)


@task
def gcs_to_bq_stg(gcs_uris, staging_table):
    loaded_staging_table = load_all_gcs_to_bq(gcs_uris, staging_table)
    return loaded_staging_table