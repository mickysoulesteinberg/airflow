from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from core.bq import format_stage_merge_query, bq_merge


@task
def bq_stg_to_final_merge(schema, merge_cols, staging_table, final_table):
    
    # TODO generate staging and final table here
    bq_merge(schema = schema, merge_cols = merge_cols, staging_table = staging_table, final_table = final_table)

    return final_table


