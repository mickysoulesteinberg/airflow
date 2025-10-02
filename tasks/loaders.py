from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import textwrap

def gcs_to_bq(
        # arguments
):
    return GCSToBigQueryOperator(

    )

def format_stage_merge_query(staging_table, final_table, schema, merge_cols):
    schema_cols = [col['name'] for col in schema]
    on_clause = ' AND '.join([f'F.{col} = S.{col}' for col in merge_cols])
    update_clause = ',\n    '.join([
        'last_updated = CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'{col} = S.{col}'
        for col in schema_cols
        if col not in merge_cols
    ])
    insert_cols = ', '.join(schema_cols)
    values_clause = ', '.join([
        'CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'S.{col}'
        for col in schema_cols
    ])

    query = f'''
    MERGE `{final_table}` F
    USING `{staging_table}` S
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols})
    VALUES ({values_clause});
    '''

    return textwrap.dedent(query).strip()


def bq_stage_merge(
        task_id = None,
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
        return BigQueryInsertJobOperator(
            task_id = task_id,
            configuration = {
                'query': {
                    'query': format_stage_merge_query(staging_table, final_table, schema, merge_cols),
                    'useLegacySql': False
                }
            }
        )
    
    return _task