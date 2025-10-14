from airflow.decorators import task
from pipeline_utils.transform import gcs_transform_and_store
from core.gcs import parse_gcs_input
import logging

logger = logging.getLogger(__name__)


@task
def gcs_transform_for_bigquery(gcs_input, table_config, json_root=None, delimiter=None,
                               bucket_name=None, new_dir=None):
    '''
    Transforms raw data in GCS to a format suitable for loading
    into BigQuery, and writes the transformed data back to a temporary
    GCS Location. Returns the new GCS path and URI.

    gcs_input can be:
    - a single GCS path
    - a list of GCS paths
    - a prefix ending with '/' (to indicate all files in a folder)
    - a wildcard path, e.g. 'path/*.json'
    '''
    logger.warning(f'gcs_transform_for_bigquery: gcs_input={gcs_input}, bucket_name={bucket_name}, new_dir={new_dir}')

    gcs_paths = []
    if isinstance(gcs_input, str):
        # Parse string input to get list of files
        gcs_input = [gcs_input]
    elif not isinstance(gcs_input, list):
        raise ValueError('gcs_input must be a string or a list of strings')
    
    gcs_paths = [f for path in gcs_input for f in parse_gcs_input(path, bucket_name=bucket_name, return_type = 'path')]
    logger.debug(f'Parsed gcs_input list to paths: {gcs_paths}')

    transformed_uris = gcs_transform_and_store(gcs_paths, table_config=table_config,
                                               json_root=json_root, delimiter=delimiter,
                                               bucket_name=bucket_name, new_dir=new_dir)


    return transformed_uris
