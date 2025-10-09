import os
import logging
from utils.config import CONFIG

logger = logging.getLogger(__name__)

# These will be used by both BQ and GCS modules
DEFAULT_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DEFAULT_BUCKET = CONFIG.get('gcs_bucket')


def resolve_project(project_id=None):
    '''
    Returns an explicit project_id, falling back to env var.
    Raises if not found (to avoid ambiguous defaults).
    '''
    resolved = project_id or DEFAULT_PROJECT_ID
    if not resolved:
        raise ValueError('Project ID not provided and GCP_PROJECT_ID is not set in environment.')
    return resolved


def resolve_bucket(bucket_name=None):
    '''
    Returns an explicit bucket name, falling back to env var.
    '''
    resolved = bucket_name or DEFAULT_BUCKET
    if not resolved:
        raise ValueError('Bucket name not provided and gcs_bucket is not set for current environment in settings.yaml.')
    return resolved
