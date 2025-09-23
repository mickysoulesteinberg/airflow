from google.cloud import bigquery
import os, logging
from utils.config import CFG


logger = logging.getLogger(__name__)
#logging.basicConfig(level = logging.INFO)
PROJECT = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project = PROJECT)

def insert_rows(api: str, table: str, rows: list[dict]):
    '''
    Generic BigQuery insert
    
    Args:
        api: which API config to use (e.g. 'tmdb', 'spotify')
        table: table name
        rows: list of dicts to insert
    '''
    dataset = CFG[api]['bq_dataset_staging']
    table_id = f'{PROJECT}.{dataset}.{table}'

    if not rows:
        logger.info(f'[BQ] No rows to insert into {table_id}')
        return  

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logger.error(f'[BQ] Insert errors for {table_id}: {errors}')
        raise RuntimeError(errors)
    
    logger.info(f'[BQ] Inserted {len(rows)} rows into {table_id}')
    return

