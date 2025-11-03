'''Functions which can be used directly in tasks, usually go here so we can add the with_config wrapper'''

from config.config_wrapper import with_config
from core.utils import resolve_bq_dataset_table, get_table_details_from_schema
from core.bq import create_table_from_schema_config, bq_merge

from config.logger import get_logger
logger = get_logger(__name__)



@with_config(['bigquery_config'])
def create_staging_table(config=None, *,
                            # bigquery_config
                            bigquery_schema=None,
                            bigquery_dataset=None,
                            bigquery_table=None,
                            # optional overrides
                            staging_table=None,
                            force_recreate=False,
                            confirm_creation=True
                            ):
    '''Creates a blank staging table to load data to. Requires confirmation of table creation before returning.'''
    logger.debug(f'create_staging_table: Creating staging table with %s',
                 f'staging_table={staging_table}' if staging_table else f'final_table={bigquery_table}')
    
    staging_table = staging_table or bigquery_table+'_stg'
    staging_dataset_table, _, _ = resolve_bq_dataset_table(table=staging_table, dataset=bigquery_dataset)

    created_staging_table = create_table_from_schema_config(dataset_table=staging_dataset_table, schema=bigquery_schema,
                                                     force_recreate=True, confirm_creation=True)

    return created_staging_table

@with_config(['bigquery_config'])
def bq_stg_to_final_merge(staging_table, config=None, *,
                          # bigquery_config
                          bigquery_schema=None,
                          bigquery_dataset=None,
                          bigquery_table=None):
    
    # Get correctly formatted dataset.table
    staging_dataset_table, _, _ = resolve_bq_dataset_table(table=staging_table, dataset=bigquery_dataset)
    final_dataset_table, _, _ = resolve_bq_dataset_table(table=bigquery_table, dataset=bigquery_dataset)

    # Create final table if it doesn't exist
    create_table_from_schema_config(dataset_table=final_dataset_table, schema=bigquery_schema,
                                    force_recreate=False, confirm_creation=True)
    
    # Get merge columns from the schema
    _, _, merge_cols = get_table_details_from_schema(bigquery_schema)

    # Perform merge
    final_table = bq_merge(schema=bigquery_schema, merge_cols=merge_cols,
                           staging_table=staging_dataset_table, final_table=final_dataset_table)

    return final_table