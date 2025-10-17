import os
from core.logger import get_logger
from utils.config import CONFIG

logger = get_logger(__name__)

# These will be used by both BQ and GCS modules
DEFAULT_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DEFAULT_BUCKET = CONFIG.get('gcs_bucket')
AIRFLOW_RAW_BUCKET = CONFIG.get('airflow_raw_bucket')
AIRFLOW_TMP_BUCKET = CONFIG.get('airflow_tmp_bucket')


def resolve_project(project_id=None):
    '''
    Returns an explicit project_id, falling back to env var.
    Raises if not found (to avoid ambiguous defaults).
    '''
    resolved = project_id or DEFAULT_PROJECT_ID
    logger.trace(f'resolve_project: input project_id={project_id}, resolved={resolved}')
    if not resolved:
        raise ValueError('Project ID not provided and GCP_PROJECT_ID is not set in environment.')
    return resolved

def resolve_bucket(purpose='raw', override=None):
    '''Resolves correct GCS bucket for Airflow operations. Allows manual declaration of bucket name with override, for read-only ingestion.'''
    if purpose == 'tmp':
        bucket_name = override or AIRFLOW_TMP_BUCKET
    elif purpose == 'raw':
        bucket_name = override or AIRFLOW_RAW_BUCKET
    else:
        raise ValueError(f'Invalid purpose: {purpose}. Expected "raw" or "tmp".')
    logger.trace(f'resolve_bucket: purpose={purpose}, override={override}, resolved={bucket_name}')
    return bucket_name

def resolve_default_bucket(bucket_name=None):
    '''
    Returns an explicit bucket name, falling back to env var.
    '''
    resolved = bucket_name or DEFAULT_BUCKET
    if not resolved:
        raise ValueError('Bucket name not provided and gcs_bucket is not set for current environment in settings.yaml.')
    return resolved

