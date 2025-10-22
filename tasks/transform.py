from airflow.decorators import task
from pipeline.transform import gcs_transform_and_store_og, gcs_transform_and_store
from config.datasources import BQ_METADATA_COL, RAW_DATA_KEY
from config.logger import get_logger
from core.utils import collect_list

logger = get_logger(__name__)


@task(multiple_outputs=True)
def setup_for_bq(config=None, **kwargs):
    
    config = config or {}
    table_config=kwargs.get('table_config') or config.get('table_config')
    logger.debug(f'setup_for_transform: table_config={table_config}')

    for key, value in config.items():
        logger.trace(f'value for {key}={value}')

    schema_config = kwargs.get('schema_config') or table_config['schema']
    dataset = kwargs.get('bigquery_dataset') or config.get('bigquery_dataset')

    return_data = {
        'table_config': table_config,
        'schema_config': schema_config,
        'dataset': dataset
    }

    api_root = kwargs.get('api_root') or config.get('api_root')
    if api_root:
        return_data['api_root'] = api_root
    source_type = kwargs.get('source_type') or config.get('source_type')
    if source_type:
        return_data['source_type'] = source_type

    return return_data



@task
def gcs_transform_for_bq(gcs_input, table_config=None,
                         schema_config=None, source_type=None,
                         data_root=RAW_DATA_KEY, metadata_root=BQ_METADATA_COL,
                         api_root=None, delimiter=None, fieldnames=None,
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
    logger.debug(f'gcs_transform_for_bigquery: gcs_input={gcs_input}')
    logger.debug(table_config)

    schema_config = schema_config or table_config.get('schema')
    if not schema_config:
        raise ValueError('Schema must be provided via schema_config or table_config')
    
    source_type = source_type or table_config.get('source_type')
    delimiter = delimiter or table_config.get('delimiter')
    fieldnames = fieldnames or table_config.get('fieldnames')

    # Get the root for the raw data in the GCS file
    full_data_root = collect_list(data_root, api_root)

    # Perform transformation and write to the new GCS location
    transformed_uris = gcs_transform_and_store(gcs_input, schema_config,
                                               data_root=full_data_root,
                                               metadata_root=metadata_root,
                                               delimiter=delimiter,
                                               new_dir=new_dir, source_bucket_override=source_bucket)


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
