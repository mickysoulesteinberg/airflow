from google.cloud import storage
import json, logging, os
from utils.config import CONFIG

# CONFIG variables
BUCKET = CONFIG.get('gcs_bucket')
if not BUCKET:
    raise ValueError('No gcs_bucket configured for current environment in settings.yaml')

# Environment variables
PROJECT_ID = os.getenv('GCS_PROJECT_ID')

logger = logging.getLogger(__name__)

#  Small functions that mean nothing now but might be useful in the future
def get_gcs_client(project_id = PROJECT_ID):
    '''Creates and returns a new GCS client.'''
    logger.debug('Creating new client')
    return storage.Client(project_id = project_id) if project_id else storage.Client()


def upload_to_gcs(path, data, wrap = True, project_id = None):

    # Get URI and prep data
    bucket_name = BUCKET
    uri = f'gs://{bucket_name}/{path}'
    if wrap:
        data = {'uri': uri, 'data': data}

    # Uploads data to gcs and returns the uri
    client = get_gcs_client(project_id = project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data), content_type = 'application/json')

    logger.info(f'[GCS] Wrote file: {uri}')

    return uri


def load_json_from_gcs(path, project_id=None):
    '''Helper: read JSON from a GCS path like 'bucket/folder/file.json'.'''
    client = get_gcs_client(project_id = project_id)
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(path)
    data = blob.download_as_text()
    return json.loads(data)
