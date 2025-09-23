from google.cloud import bigquery
import os
import logging
from utils.config import CFG

logger = logging.getLogger(__name__)
PROJECT = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project=PROJECT)


# ----------------------------------------------------------------------
# Internal helper
# ----------------------------------------------------------------------

def _status_table(api: str, dag_id: str, task_id: str, dataset_key: str = 'bq_dataset_meta') -> str:
    '''
    Resolve fully-qualified table ID for a status table.
    Table format: <project>.<dataset>.<dag_id>__<task_id>_status
    '''
    dataset = CFG['airflow'][dataset_key]
    return f'{PROJECT}.{dataset}.{dag_id}__{task_id}_status'


# ----------------------------------------------------------------------
# Setup functions (one-time / infrequent use)
# ----------------------------------------------------------------------

def create_status_table(api: str, dag_id: str, task_id: str, id_col: str = 'id', dataset_key: str = 'bq_dataset_meta'):
    '''
    Create a BigQuery status table for a specific DAG/task if it doesn't exist.
    '''
    table = _status_table(api, dag_id, task_id, dataset_key)

    ddl = f'''
    CREATE TABLE IF NOT EXISTS `{table}` (
        {id_col} INT64 NOT NULL,
        status STRING NOT NULL,
        last_page_loaded INT64,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    OPTIONS(description='Airflow checkpoint table for {dag_id}.{task_id}')
    '''
    client.query(ddl).result()
    logger.info(f'[StatusSetup] Created table if not exists: {table}')


def seed_status_table(api: str, dag_id: str, task_id: str, id_values: list[int], id_col: str = 'id', dataset_key: str = 'bq_dataset_meta'):
    '''
    Seed a status table with identifier values (default status = not_started).
    '''
    table = _status_table(api, dag_id, task_id, dataset_key)
    rows = [{id_col: val, 'status': 'not_started'} for val in id_values]

    errors = client.insert_rows_json(table, rows)
    if errors:
        logger.error(f'[StatusSetup] Errors seeding {table}: {errors}')
        raise RuntimeError(errors)

    logger.info(f'[StatusSetup] Seeded {len(rows)} rows into {table}')


# ----------------------------------------------------------------------
# Runtime functions (called by DAG tasks)
# ----------------------------------------------------------------------

def get_next_row(api: str, dag_id: str, task_id: str, id_col: str = 'id', dataset_key: str = 'bq_dataset_meta') -> dict | None:
    '''
    Fetch the next row from a task-specific status table
    where status is not_started or in_progress.
    '''
    table = _status_table(api, dag_id, task_id, dataset_key)
    query = f'''
        SELECT *
        FROM `{table}`
        WHERE status IN ('not_started', 'in_progress')
        ORDER BY {id_col}
        LIMIT 1
    '''
    results = list(client.query(query).result())
    return dict(results[0]) if results else None


def update_status(api: str, dag_id: str, task_id: str, row_id: int,
                  status: str, id_col: str = 'id', last_page: int | None = None,
                  dataset_key: str = 'bq_dataset_meta'):
    '''
    Update status for a given row in a task-specific status table.
    '''
    table = _status_table(api, dag_id, task_id, dataset_key)
    query = f'''
        UPDATE `{table}`
        SET status = @status,
            last_page_loaded = @last_page,
            updated_at = CURRENT_TIMESTAMP()
        WHERE {id_col} = @row_id
    '''
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('status', 'STRING', status),
            bigquery.ScalarQueryParameter('last_page', 'INT64', last_page),
            bigquery.ScalarQueryParameter('row_id', 'INT64', row_id),
        ]
    )
    client.query(query, job_config=job_config).result()
    logger.info(f'[Status] Updated {table}: {id_col}={row_id}, status={status}')
