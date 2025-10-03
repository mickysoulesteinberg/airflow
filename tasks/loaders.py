from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from core.bq import format_stage_merge_query


def gcs_to_bq(
        # arguments
):
    return GCSToBigQueryOperator(

    )




def bq_stage_merge(
        task_id,
        staging_table = None,
        final_table = None,
        schema = None,
        merge_cols = None
):
    
    def _task(
            task_id = task_id,
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