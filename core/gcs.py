from google.cloud import storage
import json, logging, fnmatch
from contextlib import contextmanager
from core.env import resolve_project, resolve_bucket
from core.utils import resolve_gcs_file, extract_gcs_prefix, resolve_gcs_path

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
def with_gcs_client(func):
    '''
    Decorator that injects a managed GCS client and project_id.
    Handles cases where:
      - A full client object is passed (infers project)
      - Only a project_id is passed
      - Neither is passed (falls back to env vars)
    '''
    def wrapper(*args, client=None, project_id=None, **kwargs):
        logger.debug(f'with_bucket: client={"yes" if client else "no"}')

        # If client is passed, create the bucket and populate other arguments
        if client:
            return func(*args, client=client, project_id = client.project, **kwargs)

        # Otherwise, manage client lifecycle 
        with gcs_client_context(project_id) as managed_client:
            return func(*args, client=managed_client, project_id=managed_client.project, **kwargs)

    return wrapper

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
def upload_json_to_gcs(data, path, wrap=True, new_line=False,
                       client=None, project_id=None,
                       bucket=None, bucket_name=None):
    uri = f'gs://{bucket_name}/{path}'
    if wrap:
        data = {'uri': uri, 'data': data}
    data_string = ''
    if new_line:
        data_string = '\n'.join(json.dumps(r) for r in data)
    else:
        data_string = json.dumps(data)
    
    blob = bucket.blob(path)
    blob.upload_from_string(data_string, content_type='application/json')

    logger.info(f'[GCS] Wrote file: {uri}')

    return uri

# -------------------------------------------------
# Reading
# -------------------------------------------------
@with_bucket
def load_file_from_gcs(path, client=None, project_id=None, bucket=None, bucket_name=None):
    blob = bucket.blob(path)
    data = blob.download_as_text()
    return data

@with_bucket
def list_gcs_files(prefix, client=None, project_id=None, bucket=None, bucket_name=None):
    '''Lists all blobs under a GCS folder path'''
    logger.debug(f'list_gcs_files: prefix={prefix}, bucket_name={bucket_name}')
    blobs = bucket.list_blobs(prefix=prefix)
    files = [f'gs://{bucket_name}/{blob.name}' for blob in blobs]
    return files

# -------------------------------------------------
# Deleting
# -------------------------------------------------
@with_bucket
def delete_gcs_file(path, client=None, project_id=None, bucket=None, bucket_name=None):
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



# -------------------------------------------------
# Formatting
# -------------------------------------------------
@with_gcs_client
def parse_gcs_input(input_str, client=None, project_id=None, bucket_name=None, return_type='uri'):
    '''Parses a GCS input string (path, prefix, wildcard) and returns a list of uris'''
    logger.debug(f'parse_gcs_input: input_str={input_str}')

    if return_type not in ['uri', 'path']:
        raise ValueError('return_type must be "uri" or "path"')
    
    path_str, uri_str, bucket_name = resolve_gcs_file(input_str, bucket_name=bucket_name)
    logger.debug(f'Resolved gcs_input to: bucket_name={bucket_name}, path_str={path_str}, gcs_uri={uri_str}')

    # Get prefix and list all files under it
    prefix = extract_gcs_prefix(path_str)
    logger.debug(f'Prefix for listing: {prefix}')
    all_uris = list_gcs_files(prefix, client=client, project_id=project_id, bucket_name=bucket_name)
    logger.debug(f'All files under prefix: {all_uris}')

    # Filter files to matching
    uri_list = [
        u for u in all_uris
        if fnmatch.fnmatch(u, uri_str)
    ]

    if return_type == 'uri':
        return uri_list
    else:
        return [resolve_gcs_path(u) for u in uri_list]
    

