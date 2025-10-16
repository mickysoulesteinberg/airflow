from core.gcs import delete_files, with_gcs_client
from pipeline.utils import parse_gcs_input
import logging

logger = logging.getLogger(__name__)


@with_gcs_client
def delete_gcs_files(gcs_input, client=None, project_id=None, bucket_name=None):
    '''
    Deletes GCS files.
    gcs_input can be a single path, a list of paths, a prefix, or a wildcard pattern.
    Use wait_for to ensure this task runs after other tasks.
    '''
    _, paths, bckt_name = parse_gcs_input(gcs_input, client=client, project_id=project_id, bucket_name=bucket_name)
    
    delete_files(paths, client=client, project_id=project_id, bucket_name=bckt_name)
    logger.info(f'Deleted {len(paths)} files from bucket: {bckt_name}')

    return