from airflow.decorators import task
from core.bq import bq_merge


@task
def bq_stg_to_final_merge(schema, merge_cols, staging_table, final_table):
    
    # TODO generate staging and final table here
    bq_merge(schema = schema, merge_cols = merge_cols, staging_table = staging_table, final_table = final_table)

    return final_table


