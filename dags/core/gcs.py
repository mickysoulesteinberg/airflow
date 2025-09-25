from google.cloud import storage
import json, logging
from utils.config import CONFIG

logger = logging.getLogger(__name__)
client = storage.Client()


# Probably delete or replace
def write_json(api: str, data: dict, path: str, dag_id: str):
    bucket_name = CFG[api]['gcs_bucket']
    bucket = client.bucket(bucket_name)
    full_path = f'{dag_id}/{path}'

    blob = bucket.blob(full_path)
    blob.upload_from_string(json.dumps(data), content_type = 'application/json')

    logger.info(f'[GCS] Wrote file: gs://{bucket_name}/{full_path}')

def upload_to_gcs(data, bucket, dag_id = None, group_id = None):
    print('Uploading to GCS (This function is in progress)')
    return