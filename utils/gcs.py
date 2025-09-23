from google.cloud import storage
import json, logging
from utils.config import CFG

logger = logging.getLogger(__name__)
client = storage.Client()

def write_json(api: str, data: dict, path: str, dag_id: str):
    bucket_name = CFG[api]['gcs_bucket']
    bucket = client.bucket(bucket_name)
    full_path = f'{dag_id}/{path}'

    blob = bucket.blob(full_path)
    blob.upload_from_string(json.dumps(data), content_type = 'application/json')

    logger.info(f'[GCS] Wrote file: gs://{bucket_name}/{full_path}')

def write_success_marker(api: str, data_type: str, year: int, dag_id: str):
    '''
    Write a _SUCCESS marker for a completed partition.
    
    Args:
        api: which API config to use (e.g. 'tmdb', 'spotify')
        data_type: e.g. 'movies' or 'credits'
        year: year of the data partition
        dag_id: Airflow DAG ID
    '''
    bucket_name = CFG[api]['gcs_bucket']
    bucket = client.bucket(bucket_name)
    path = f'{dag_id}/{data_type}/year={year}/_SUCCESS'

    blob = bucket.blob(path)
    blob.upload_from_string('', content_type = 'text/plain') 

    logger.info(f'[GCS] Wrote success marker: gs://{bucket_name}/{path}')