from airflow.decorators import task
from core.bq import bq_merge, create_table


@task
def bq_stg_to_final_merge(schema, merge_cols, staging_table, final_table):
    bq_merge(schema=schema, merge_cols=merge_cols, 
             staging_table=staging_table, final_table=final_table)
    return

@task
def create_staging_table(dataset_table, schema_config):
    '''Creates a blank staging table to load data to. Requires confirmation of table creation before returning.'''
    staging_table = create_table(dataset_table=dataset_table, schema_config=schema_config,
                                    force_recreate=True, confirm_creation=True)
    return staging_table


