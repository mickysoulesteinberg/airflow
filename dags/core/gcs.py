from google.cloud import storage
import json, logging, os
from utils.config import CONFIG

# CONFIG variables
BUCKET = CONFIG.get('gcs_bucket')
if not BUCKET:
    raise ValueError('No gcs_bucket configured for current environment in settings.yaml')

logger = logging.getLogger(__name__)

#  Small functions that mean nothing now but might be useful in the future
def get_gcs_client():
    '''Creates and returns a new GCS client.'''
    logger.debug('Creating new client')
    return storage.Client()

# def build_gcs_path(dag_id, api, api_path, gcs_file_name):
#     # Builds the gcs path based on context
#     gcs_path = f'{dag_id}/{api}/{api_path}/{gcs_file_name}'
#     logger.warning(f'gcs_path={gcs_path}')
#     return gcs_path

def upload_to_gcs(gcs_path, data, gcs_bucket = None):
    # Uploads data to gcs and returns the uri
    client = get_gcs_client()
    bucket_name = gcs_bucket or BUCKET
    blob_name = gcs_path
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type = 'application/json')
    uri = f'gs://{bucket_name}/{blob_name}'

    logger.info(f'[GCS] Wrote file: {uri}')

    return uri


