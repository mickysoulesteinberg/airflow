from core.gcs import load_file_from_gcs, upload_json_to_gcs, with_bucket
from core.utils import resolve_gcs_uri
import os, json, csv, logging
from io import StringIO

logger = logging.getLogger(__name__)

def transform_record(record, schema_config, context_values=None):
    context_values = context_values or {}
    row = {}
    for col in schema_config:
        name = col['name']
        source = col.get('source', name)
        col_type = col.get('type')

        # Set default value from context if set
        if name in context_values:
            value = context_values[name]
            logger.debug(f'Setting value for {name} from context: {value}')
        else:
            value = record.get(source)
        
        if col_type == 'JSON' and value is not None:
            value = json.dumps(value)

        row[name] = value
    return row


def transform_csv_records(content, schema_config, context_values=None, delimiter=None, fieldnames=None):
    delimiter = delimiter or ','
    reader = csv.DictReader(StringIO(content), fieldnames=fieldnames, delimiter=delimiter)
    csv_data = list(reader)
    logger.debug(f'csv_data sample: {csv_data[:2]}')

    rows = [transform_record(record, schema_config, context_values=context_values) for record in csv_data]
    return rows

def transform_json_records(content, schema_config, context_values={}, json_root=None):
    json_data = json.loads(content)

    # Drill down if json_root is provided
    if json_root:
        for key in json_root:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f'Key {key} not found in JSON data')

    # At this point, json_data could be a dict (single record) or list (multiple records)
    if not isinstance(json_data, list):
        json_data = [json_data]
    rows = [transform_record(record, schema_config, context_values=context_values) for record in json_data]

    return rows



@with_bucket
def gcs_transform_and_store(paths, schema_config=None, table_config=None, source_type=None,
                            new_dir=None, new_file_name=None, 
                            json_root=None, delimiter=None, fieldnames=None,
                            client=None, project_id=None, bucket=None, bucket_name=None):
    '''
    Reads data from GCS (JSON or CSV/TXT), applies transform, 
    then writes transformed JSON back to GCS.
    Returns new GCS URI for downstream bulk load
    '''
    logger.debug(f'gcs_transform_and_store: paths={paths}, bucket_name={bucket_name}, new_dir={new_dir}, new_file_name={new_file_name}')
    
    if table_config:
        schema_config = schema_config or table_config.get('schema')
        source_type = source_type or table_config.get('source_type')
        delimiter = delimiter or table_config.get('delimiter')
        fieldnames = fieldnames or table_config.get('fieldnames')

    if not schema_config:
        raise ValueError('Schema must be provided via schema_config or table_config')
    
    # Handle path input as single or list
    if isinstance(paths, str):
        paths = [paths]
    elif isinstance(paths, list):
        if len(paths) > 1 and new_file_name is not None:
            logger.warning('Multiple input paths provided but new_file_name is set. Using default naming for each file.')
            new_file_name = None
    else:
        raise ValueError('paths must be a string or a list of strings')


    new_uris = []
    # Transform and store for each path
    for path in paths:

        # Get raw data
        content = load_file_from_gcs(path, bucket_name=bucket_name)

        # Define context values to use for static/generated columns
        context_values = {
            'gcs_uri': resolve_gcs_uri(path, bucket_name=bucket_name)
        }
        logger.debug(f'context_values: {context_values}')

        # Transform data depending on source type
        if source_type is None:
            # Use the file extension to determine source type
            logger.warning('source_type not provided, inferring from file extension')
            source_type = os.path.splitext(path)[-1].lower().lstring('.')
        if source_type == 'json':
            transformed_records = transform_json_records(content, schema_config, context_values=context_values,
                                                        json_root=json_root)
        elif source_type in ['csv', 'txt']:
            if not fieldnames:
                logger.warning('fieldnames not provided, inferring from CSV header')

            transformed_records = transform_csv_records(content, schema_config, context_values=context_values,
                                                        delimiter=delimiter, fieldnames=fieldnames)
        else:
            raise ValueError(f'Unsupported file extension: {source_type}')
        
        # If save directory not set, get it from the input path
        dir = '/'.join(path.split('/')[:-1])
        save_dir = new_dir or f'{dir}/tmp'
        # If new file name not set, generate from input file name
        save_file_name = new_file_name or os.path.basename(path).replace(f'.{source_type}', f'_transformed.{source_type}')
        new_blob_path = f'{save_dir}/{save_file_name}'
    
        # Write to new GCS location
        new_uri = upload_json_to_gcs(transformed_records, new_blob_path, wrap=False, new_line=True,
                                     client=client, project_id=project_id, bucket=bucket, bucket_name=bucket_name)
        new_uris.append(new_uri)

    return new_uris

