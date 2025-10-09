from google.cloud import storage
import json, logging, os
from utils.config import CONFIG
from datetime import datetime, UTC
from contextlib import contextmanager
from core.env import resolve_project, resolve_bucket

logger = logging.getLogger(__name__)

# -------------------------------------------------
# Manage Storage Client
# -------------------------------------------------

# Always used to create the client
def get_gcs_client(project_id=None):
    '''
    Creates and returns a new GCS client.
    If project_id is None, defaults from environment/credentials
    '''
    project_id = resolve_project(project_id)
    logger.debug(f'Creating new client (project_id: {project_id})')
    return storage.Client(project=project_id)

# Explicitly manage client lifecycle
@contextmanager
def gcs_client_context(project_id=None):
    '''
    Context manager that yields a GCS client and ensures it is closed.
    Usage:
        with gcs_client_context('my-project') as client:
            ...
    '''
    client = get_gcs_client(project_id=project_id)
    try:
        yield client
    finally:
        client.close()

# Decorator wraps functions for Airflow
def with_bucket(func):
    '''
    Decorator that injects a managed GCS client, project_id, and bucket.
    Handles cases where:
      - A full bucket object is passed (infers client + project)
      - Only a bucket name is passed
      - Neither is passed (falls back to env vars)
    '''
    def wrapper(*args, client=None, project_id=None, bucket=None, bucket_name=None, **kwargs):
        logger.debug(f'with_bucket: client={"yes" if client else "no"}, bucket={"yes" if bucket else "no"}')

        # If bucket is passed, populate arguments with correct values
        if bucket:
            client = bucket.client
            return func(*args, client=client, project_id=client.project,
                        bucket=bucket, bucket_name=bucket.name, **kwargs)

        # Otherwise, if client is passed, create the bucket and populate other arguments
        if client:
            bucket_name = resolve_bucket(bucket_name)
            bucket = client.bucket(bucket_name)
            return func(*args, client=client, project_id = client.project,
                        bucket=bucket, bucket_name=bucket_name, **kwargs)

        # Otherwise, manage client lifecycle and create bucket
        with gcs_client_context(project_id) as managed_client:
            bucket_name = resolve_bucket(bucket_name)
            bucket = managed_client.bucket(bucket_name)
            return func(*args, client=managed_client, project_id=managed_client.project,
                        bucket=bucket, bucket_name=bucket_name, **kwargs)

    return wrapper


# -------------------------------------------------
# Uploading
# -------------------------------------------------
@with_bucket
def upload_to_gcs(path, data, wrap=True, client=None, project_id=None, bucket=None, bucket_name=None):

    # Get URI and prep data
    uri = f'gs://{bucket_name}/{path}'
    if wrap:
        data = {'uri': uri, 'data': data}

    # Uploads data to gcs and returns the uri
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data), content_type='application/json')

    logger.info(f'[GCS] Wrote file: {uri}')

    return uri


# -------------------------------------------------
# Reading
# -------------------------------------------------
@with_bucket
def load_json_from_gcs(path, client=None, project_id=None, bucket=None, bucket_name=None):
    '''Helper: read JSON from a GCS path like 'bucket/folder/file.json'.'''
    blob = bucket.blob(path)
    data = blob.download_as_text()
    return json.loads(data)


@with_bucket
def gcs_transform_and_store(schema_config, path,
                            tmp_dir=None, tmp_file_name=None,
                            json_root=None, 
                            client=None, project_id=None,
                            bucket=None, bucket_name=None):
    """
    Reads JSON from GCS, applies transform, writes transformed JSON to a temp GCS location.
    Returns the new GCS URI for downstream bulk load.
    """
    # TODO check that path goes to a json file

    # Tranform the raw json to BigQuery-ready format
    transformed_records = transform_json_records(schema_config, path, json_root=json_root, project_id=project_id)

    # Write to new GCS temp location
    folder_path = '/'.join(path.split('/')[:-1])
    tmp_folder_path = tmp_dir or f'{folder_path}/tmp'
    tmp_file_name = tmp_file_name or os.path.basename(path).replace('.json', '_transformed.json')
    tmp_blob_path = f'{tmp_folder_path}/{tmp_file_name}'
    tmp_uri = f'gs://{bucket_name}/{tmp_blob_path}'

    blob = bucket.blob(tmp_blob_path)
    blob.upload_from_string(
        '\n'.join(json.dumps(r) for r in transformed_records),
        content_type='application/json',
    )

    return {
        'tmp_path': tmp_blob_path,
        'tmp_uri': tmp_uri
    }


@with_bucket
def delete_gcs_files(paths, client=None, project_id=None, bucket=None, bucket_name=None):
    for path in paths:
        blob = bucket.blob(path)
        blob.delete()
    return

@with_bucket
def delete_gcs_folder(folder_path, client=None, project_id=None, bucket=None, bucket_name=None):
    '''Deletes all blobs under a GCS folder path'''
    blobs = bucket.list_blobs(prefix=folder_path)
    deleted = 0
    for blob in blobs:
        blob.delete()
        deleted += 1
    logger.info(f'Deleted {deleted} blobs from gs://{bucket_name}/{folder_path}')
    return

# Move below to core.transform

def transform_json_records(schema_config, gcs_path, project_id=None, json_root=None):
    json_data = load_json_from_gcs(gcs_path, project_id=project_id)

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