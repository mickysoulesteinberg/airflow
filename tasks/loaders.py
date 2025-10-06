from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from core.bq import format_stage_merge_query, bq_merge


def gcs_to_bq(
        # arguments
):
    return GCSToBigQueryOperator(

    )

@task
def bq_stg_to_final_merge(schema, merge_cols, staging_table, final_table):
    
    # TODO generate staging and final table here
    bq_merge(schema = schema, merge_cols = merge_cols, staging_table = staging_table, final_table = final_table)

    return final_table




def bq_stg_to_final_merge_og(
        task_id,
        schema = None,
        merge_cols = None,
        staging_table = None,
        table = None
):
    @task(task_id = task_id)
    def _merge_task(
        schema = schema,
        merge_cols = merge_cols,
        staging_table = staging_table,
        table = table,
        **context
    ):
        query = format_stage_merge_query(
            staging_table = staging_table,
            final_table = table,
            schema = schema,
            merge_cols = merge_cols
        )
        op = BigQueryInsertJobOperator(
            task_id = f'{task_id}_op',
            configuration = {
                'query': {
                    'query': query,
                    'useLegacySql': False
                }
            },
            do_xcom_push = True
        )
        op.execute(context = context)
        return table
    return _merge_task


def bq_stage_merge(
        task_id,
        staging_table = None,
        final_table = None,
        schema = None,
        merge_cols = None
):
    
    @task(task_id = task_id)
    def _task(
            staging_table = staging_table,
            final_table = final_table,
            schema = schema,
            merge_cols = merge_cols
    ):
        # TODO Raise errors in case of empty arguments
        query = format_stage_merge_query(staging_table, final_table, schema, merge_cols)

        return BigQueryInsertJobOperator(
            task_id = task_id,
            configuration = {
                'query': {
                    'query': query,
                    'useLegacySql': False
                }
            }
        )
    
    return _task