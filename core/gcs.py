from google.cloud import storage
from contextlib import contextmanager
from core.env import resolve_project
from config.logger import get_logger
from core.utils import resolve_gcs_uri

logger = get_logger(__name__)

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
    logger.info(f'Creating Storage Client for project {project_id}')
    return storage.Client(project=project_id)

def close_gcs_client(client):
    logger.info(f'Closing Client for project {client.project}')
    client.close()

# Explicitly manage client lifecycle
@contextmanager
def gcs_client_context(project_id=None):
    '''
    Context manager that yields a GCS client and ensures it is closed.
    Usage:
        with gcs_client_context('my-project') as client:
            ...
    '''
    logger.debug(f'gcs_client_context: Initializing client, project_id={project_id}')
    client = get_gcs_client(project_id=project_id)
    try:
        yield client
    finally:
        close_gcs_client(client)

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
        logger.debug(f'with_gcs_client: client=%s, project_id=%s',
                     'yes' if client else 'no',
                     project_id or 'none')

        # If client is passed, populate project_id
        if client:
            return func(*args, client=client, project_id = client.project, **kwargs)

        # Otherwise, manage client lifecycle 
        with gcs_client_context(project_id) as managed_client:
            return func(*args, client=managed_client, project_id=managed_client.project, **kwargs)

    return wrapper

def with_bucket(func):
    '''
    Decorator that ensures both `bucket` and `bucket_name` are resolved.
    Requires a valid GCS client (typically injected via @with_gcs_client);
    does not create or manage clients itself.
    '''
    def wrapper(*args, client=None, bucket=None, bucket_name=None, **kwargs):

        logger.debug('with_bucket: bucket=%s, bucket_name=%s',
                    'yes' if bucket else 'no',
                    bucket_name or 'none')

        # If bucket is passed, populate arguments with correct values
        if bucket:
            if not hasattr(bucket, 'name'):
                raise TypeError('Invalid bucket object passed to with_bucket')
            client = bucket.client
            if bucket_name and bucket_name != bucket.name:
                logger.warning(f'with_bucket: bucket_name argument ({bucket_name}) does not match bucket.name ({bucket.name}). Using bucket.name.')
            bucket_name = bucket.name
            logger.micro(f'bucket_name generated: {bucket_name}')

        # Otherwise use client and bucket_name to generate bucket
        elif bucket_name:
            if not client:
                raise ValueError('with_bucket requires a valid GCS client')
            bucket = client.bucket(bucket_name)
            logger.micro(f'bucket {bucket_name} initialized.')
        
        else:
            raise ValueError('with_bucket requires either `bucket` or `bucket_name` argument')

        return func(*args, client=client, bucket=bucket, bucket_name=bucket_name, **kwargs)

    return wrapper

# -------------------------------------------------
# Uploading
# -------------------------------------------------
@with_bucket
@with_gcs_client
def upload_from_string(data, path, content_type='application/json',
                       client=None, project_id=None, bucket=None, bucket_name=None):
    logger.debug(f'upload_from_string: Uploading data to %s, data=%s',
                 path, data[:100])
    blob = bucket.blob(path)
    blob.upload_from_string(data, content_type)
    uri = resolve_gcs_uri(path, bucket_name)
    return uri

# -------------------------------------------------
# Reading
# -------------------------------------------------

@with_bucket
@with_gcs_client
def load_file_from_gcs(path, client=None, project_id=None, bucket=None, bucket_name=None):
    logger.debug(f'load_file_from_gcs: path={path}')
    blob = bucket.blob(path)
    data = blob.download_as_text()
    logger.trace(f'load_file_from_gcs: loaded_content={data[:100]}')
    return data

@with_bucket
@with_gcs_client
def list_gcs_files(prefix, client=None, project_id=None, bucket=None, bucket_name=None):
    '''Lists all blobs under a GCS folder path'''
    logger.debug(f'list_gcs_files: prefix={prefix}, bucket_name={bucket_name}')
    blobs = bucket.list_blobs(prefix=prefix)
    files = [f'gs://{bucket_name}/{blob.name}' for blob in blobs]
    logger.trace(f'files returned: {len(files)}, e.g. {files[:3]}')
    return files

# -------------------------------------------------
# Deleting
# -------------------------------------------------

@with_bucket
@with_gcs_client
def delete_files(paths, client=None, project_id=None, bucket=None, bucket_name=None):
    logger.debug(f'delete_files: {len(paths)} paths provided, paths={paths}')
    for path in paths:
        blob = bucket.blob(path)
        blob.delete()
    logger.info(f'Deleted {len(paths)} from bucket {bucket_name}')
    return

@with_bucket
@with_gcs_client
def delete_gcs_prefix(prefix, client=None, project_id=None, bucket=None, bucket_name=None):
    '''Deletes all blobs under a GCS folder path'''
    logger.debug(f'delete_gcs_prefix: prefix={prefix}')
    blobs = bucket.list_blobs(prefix=prefix)
    deleted = 0
    for blob in blobs:
        blob.delete()
        deleted += 1
    logger.info(f'Deleted {deleted} blobs from gs://{bucket_name}/{prefix}')
    return