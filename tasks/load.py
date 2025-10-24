from airflow.decorators import task
from core.bq import bq_merge, load_all_gcs_to_bq
from config.logger import get_logger
from core.utils import resolve_bq_dataset_table

from pipeline.create_table import create_table_from_config

logger = get_logger(__name__)


@task
def bq_stg_to_final_merge(staging_table, final_table,
                          merge_cols=None, schema_config=None,
                          table_config=None, dataset=None):
    
    merge_cols = merge_cols or table_config.get('row_id')
    logger.debug(f'bq_stg_to_final_merge: Beginning merge from {staging_table} to {final_table}')
    
    if not merge_cols:
        raise ValueError('merge_cols must be provided')
    schema_config = schema_config or table_config.get('schema')
    if not schema_config:
        raise ValueError('schema_config must be provided')
    
    staging_dataset_table, _, _ = resolve_bq_dataset_table(table=staging_table, dataset=dataset)
    final_dataset_table, _, _ = resolve_bq_dataset_table(table=final_table, dataset=dataset)
    
    # Create final table if it doesn't exist
    create_table_from_config(dataset_table=final_dataset_table, schema_config=schema_config,
                 force_recreate=False, confirm_creation=True)

    # Perform merge
    final_table = bq_merge(schema=schema_config, merge_cols=merge_cols, 
             staging_table=staging_dataset_table, final_table=final_dataset_table)
    
    return final_table



@task
def create_staging_table(schema_config, staging_table=None, final_table=None, dataset=None):
    '''Creates a blank staging table to load data to. Requires confirmation of table creation before returning.'''
    
    logger.debug(f'create_staging_table: Creating staging table with %s',
                 f'staging_table={staging_table}' if staging_table else f'final_table={final_table}')
    
    staging_table = staging_table or final_table+'_stg'
    staging_dataset_table, _, _ = resolve_bq_dataset_table(table=staging_table, dataset=dataset)

    created_staging_table = create_table_from_config(dataset_table=staging_dataset_table, schema_config=schema_config,
                                    force_recreate=True, confirm_creation=True)
    return created_staging_table


@task
def gcs_to_bq_stg(gcs_uris, staging_table):
    loaded_staging_table = load_all_gcs_to_bq(gcs_uris, staging_table)
    return loaded_staging_table