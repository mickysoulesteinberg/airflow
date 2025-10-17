from airflow.decorators import task
from pipeline.transform import gcs_transform_and_store
from core.logger import get_logger

logger = get_logger(__name__)


@task
def gcs_transform_for_bigquery(gcs_input, table_config, json_root=None, delimiter=None,
                               source_bucket=None, new_dir=None):
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

    # Perform transformation and write to the new GCS location
    transformed_uris = gcs_transform_and_store(gcs_input, table_config=table_config,
                                               json_root=json_root, delimiter=delimiter,
                                               new_dir=new_dir, source_bucket_override=source_bucket)


    return transformed_uris
