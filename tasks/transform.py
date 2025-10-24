from airflow.decorators import task
from pipeline.transform import gcs_transform_and_store_og, gcs_transform_and_store
from config.datasources import BQ_METADATA_COL, RAW_DATA_KEY
from config.logger import get_logger
from core.utils import collect_list

logger = get_logger(__name__)



@task
def gcs_transform_for_bq(storage_config=None, gcs_path=None, gcs_bucket=None,
                         table_config=None, schema_config=None,
                         data_config=None, source_type=None, data_root=None, delimiter=None, fieldnames=None,
                         raw_data_root=RAW_DATA_KEY, metadata_root=BQ_METADATA_COL,
                         new_dir=None):
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
    logger.debug(f'storage_config={storage_config}')
    logger.debug(f'data_config={data_config}')

    # Storage Config Args
    storage_config = storage_config or {}
    gcs_path = gcs_path or storage_config.get('gcs_path')
    gcs_bucket = gcs_bucket or storage_config.get('gcs_bucket')

    # Table Config Args
    table_config = table_config or {}
    schema_config = schema_config or table_config.get('schema')
    if not schema_config:
        raise ValueError('Schema must be provided via schema_config or table_config')
    
    # Data Config Args
    data_config = data_config or {}
    source_type = source_type or data_config.get('source_type')
    delimiter = delimiter or data_config.get('delimiter')
    fieldnames = fieldnames or data_config.get('fieldnames')
    logger.micro(f'fieldnames={fieldnames}')
    data_root = data_root or data_config.get('data_root')

    # Get the root for the raw data in the GCS file
    full_data_root = collect_list(raw_data_root, data_root)

    # Perform transformation and write to the new GCS location
    transformed_uris = gcs_transform_and_store(gcs_path, schema_config,
                                               source_type=source_type, delimiter=delimiter, fieldnames=fieldnames,
                                               data_root=full_data_root,
                                               metadata_root=metadata_root,
                                               new_dir=new_dir, source_bucket_override=gcs_bucket)


    return transformed_uris


@task
def gcs_transform_for_bigquery_og(gcs_input, table_config, json_root=None, delimiter=None,
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
    logger.debug(f'gcs_transform_for_bigquery: gcs_input={gcs_input}')
    logger.trace(f'gcs_transform_for_bigquery: source_bucket=%s, new_dir=%s, json_root=%s',
                 source_bucket, new_dir, json_root)
    logger.trace(f'gcs_transform_for_bigquery: table_config={table_config}')
    transformed_uris = gcs_transform_and_store_og(gcs_input, table_config=table_config,
                                               json_root=json_root, delimiter=delimiter,
                                               new_dir=new_dir, source_bucket_override=source_bucket)


    return transformed_uris
