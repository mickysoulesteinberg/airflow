from airflow.decorators import task
from core.bq import bq_merge, load_all_gcs_to_bq
from core.logger import get_logger

from pipeline.create_table import create_table_from_config

logger = get_logger(__name__)

@task
def bq_stg_to_final_merge(schema, staging_table, final_table, merge_cols):
    if not merge_cols:
        raise ValueError('merge_cols must be provided')
    
    # Create final table if it doesn't exist
    create_table_from_config(dataset_table=final_table, schema_config=schema,
                 force_recreate=False, confirm_creation=True)

    # Perform merge
    final_table = bq_merge(schema=schema, merge_cols=merge_cols, 
             staging_table=staging_table, final_table=final_table)
    
    return final_table

@task
def create_staging_table(dataset_table, schema_config):
    '''Creates a blank staging table to load data to. Requires confirmation of table creation before returning.'''
    staging_table = create_table_from_config(dataset_table=dataset_table, schema_config=schema_config,
                                    force_recreate=True, confirm_creation=True)
    return staging_table

@task
def gcs_to_bq_stg(gcs_uris, staging_table):
    loaded_staging_table = load_all_gcs_to_bq(gcs_uris, staging_table)
    return loaded_staging_table