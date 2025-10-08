from google.cloud import storage
import json, logging, os
from utils.config import CONFIG
from datetime import datetime, UTC


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
    client.close()

    return uri


def load_json_from_gcs(path, project_id=None):
    '''Helper: read JSON from a GCS path like 'bucket/folder/file.json'.'''
    client = get_gcs_client(project_id = project_id)
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(path)
    data = blob.download_as_text()
    return json.loads(data)

def gcs_transform_and_store(schema_config, path, tmp_dir='tmp', json_root = None, project_id = None):
    """
    Reads JSON from GCS, applies transform, writes transformed JSON to a temp GCS location.
    Returns the new GCS URI for downstream bulk load.
    """
    # TODO check that path goes to a json file

    # Tranform the raw json to BigQuery-ready format
    transformed_records = transform_json_records(schema_config, path, json_root = json_root, project_id = project_id)

    # Write to new GCS temp location
    bucket_name = BUCKET
    folder_path = '/'.join(path.split('/')[:-1])
    file_name = os.path.basename(path).replace('.json', '_transformed.json')
    tmp_blob_path = f'{folder_path}/{tmp_dir}/{file_name}'
    tmp_uri = f'gs://{bucket_name}/{tmp_blob_path}'

    client = get_gcs_client(project_id = project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(tmp_blob_path)
    blob.upload_from_string(
        '\n'.join(json.dumps(r) for r in transformed_records),
        content_type='application/json',
    )
    client.close()

    return {
        # 'original_path': path,
        # 'bucket_name': bucket_name,
        # 'folder_path': folder_path,
        # 'file_name': file_name,
        'tmp_path': tmp_blob_path,
        'tmp_uri': tmp_uri
    }



def delete_gcs_files(paths):
    client = get_gcs_client()
    bucket = client.bucket(BUCKET)
    for path in paths:
        blob = bucket.blob(path)
        blob.delete()
    client.close()




# Move below to core.transform

def transform_json_records(schema_config, gcs_path, project_id = None, json_root = None):
    json_data = load_json_from_gcs(gcs_path, project_id)

    # Define context values to use for static/generated columns
    context_values = {
        'gcs_uri': gcs_path,
        'last_updated': bq_current_timestamp()
    }

    # Drill down if json_root is provided
    if json_root:
        for key in json_root:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f"json_root step '{key}' not found in JSON at {gcs_path}")

    # At this point, json_data could be a dict (single record) or list (multiple records)
    if isinstance(json_data, list):
        rows = [transform_record(record, schema_config, context_values) for record in json_data]
    else:
        rows = [transform_record(json_data, schema_config, context_values)]

    return rows


def transform_record(record, schema_config, context_values):
    row = {}
    for col in schema_config:
        name = col['name']
        json_path = col.get('json_path')
        col_type = col.get('type')

        if json_path:
            value = record.get(json_path)
        elif name in context_values:
            value = context_values[name]
        else:
            value = None

        if col_type == 'JSON' and value is not None:
            value = json.dumps(value)

        row[name] = value
    return row




# Move below to "helpers" or similar

def bq_current_timestamp():
    return datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')