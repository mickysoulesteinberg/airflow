from core.gcs import load_file_from_gcs, with_gcs_client, upload_from_string
from core.utils import resolve_gcs_uri, extract_gcs_prefix, join_gcs_path, collect_list, extract_gcs_file_name
from pipeline.utils import parse_gcs_input
import os, json, csv
from io import StringIO
from config.logger import get_logger
from core.env import resolve_bucket
from config.datasources import BQ_METADATA_COL

logger = get_logger(__name__)

def transform_record(record, schema_config, context_values=None,  metadata=None):
    logger.micro(f'transform_record: record={record}')
    context_values = context_values or {}
    metadata =  metadata or {}
    row = {}
    for col in schema_config:
        name = col['name']
        source = col.get('source', name)

        # Set default value from context if set
        if name == BQ_METADATA_COL:
            value = metadata
        elif name in context_values:
            value = context_values[name]
            logger.micro(f'Setting value for {name} from context: {value}')
        else:
            value = record.get(source)

        row[name] = value
    logger.micro(f'transform_record: transformed = {row}')
    return row




def json_drill_down(data, root):
    for key in root:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            raise KeyError(f'Key {key} not found in JSON data')
    return data

def get_context_value(key, path=None):
    if key=='FILE_NAME':
        value = extract_gcs_file_name(path)
    logger.trace(f'get_context_value: {key}: {value}')
    return value

def get_context_values(schema_config, path=None):
    context_values = {}
    for col in schema_config:
        logger.trace(f'get_context_values: col={col}')
        if col.get('source')=='CONTEXT':
            name = col['name']
            logger.trace(name)
            context_values[name] = get_context_value(name, path=path)
    logger.trace(f'get_context_values: path={path}, context_values={context_values}')
    return context_values

@with_gcs_client
def gcs_transform_and_store_file(path, schema_config,
                                 source_type=None,
                                 new_dir=None, new_file_name=None,
                                 data_root=None, metadata_root=None,
                                 delimiter=None, fieldnames=None, 
                                 metadata=None,
                                 client=None, project_id=None,
                                 source_bucket_override=None, store_bucket_override=None):
    
    # Handle variables
    metadata_root = collect_list(metadata_root)
    data_root = collect_list(data_root)
    
    # Resolve Buckets
    source_bucket = resolve_bucket(override=source_bucket_override)
    store_bucket = resolve_bucket(purpose='tmp', override=store_bucket_override)
    
    # Get raw data
    content = load_file_from_gcs(path, client=client, project_id=project_id, bucket_name=source_bucket)

    # Update metadat
    metadata = metadata or {}
    metadata['gcs_path'] = path
    logger.trace(f'gcs_transform_and_store_file: Updating metadata with gcs_path: {path}')

    context_values = get_context_values(schema_config, path=path)

    # Transform data depending on source type
    if source_type is None:
        # Use the file extension to determine source type
        logger.warning('gcs_transform_and_store_file: source_type not provided, inferring from file extension')
        source_type = os.path.splitext(path)[-1].lower().lstrip('.')
    if source_type == 'json':

        logger.debug(f'gcs_transform_and_store_file: Beginning transform of data={content[:100]}')
        json_data = json.loads(content)

        source_data = json_drill_down(json_data, data_root)
        logger.trace(f'gcs_transform_and_store_file: Drilled down to root {data_root} json_data={source_data}')

        if metadata_root:
            source_metadata = json_drill_down(json_data, metadata_root)
            metadata['source_metadata'] = source_metadata

        transformed_records = [transform_record(record, schema_config, metadata=metadata, context_values=context_values) for record in collect_list(source_data)]
        logger.trace(f'gcs_transform_and_store_file: transformed rows sample = {transformed_records[:2]}')


    elif source_type in ['csv', 'txt']:
        if not fieldnames:
            logger.warning('gcs_transform_and_store_file: fieldnames not provided, inferring from CSV header')
        if metadata_root:
            logger.warning('gcs_transform_and_store_file: metadata_root provided, but not used for csv files')

        logger.debug(f'gcs_transform_and_store_file: Beginning transform of data={content[:100]}')
        delimiter = delimiter or ','
        reader = csv.DictReader(StringIO(content), fieldnames=fieldnames, delimiter=delimiter)
        csv_data = list(reader)
        logger.trace(f'gcs_transform_and_store_file: csv_data sample = {csv_data[:2]}')

        transformed_records = [transform_record(record, schema_config, metadata=metadata, context_values=context_values) for record in csv_data]
        logger.trace(f'gcs_transform_and_store_file: transformed rows sample={transformed_records[:2]}')

    else:
        raise ValueError(f'Unsupported file extension: {source_type}')
    
    # Prep data for BigQuery
    data_to_load = '\n'.join(json.dumps(r) for r in transformed_records)
    
    # If save directory not set, get it from the input path
    if new_dir:
        save_dir = new_dir
    else:
        save_dir = join_gcs_path(extract_gcs_prefix(path))
    # If new file name not set, generate from input file name
    save_file_name = new_file_name or os.path.basename(path)
    new_blob_path = join_gcs_path(save_dir, save_file_name)

    # Write to new GCS location
    upload_from_string(data_to_load, new_blob_path, client=client, project_id=project_id,
                       bucket_name=store_bucket)
    store_uri = resolve_gcs_uri(new_blob_path, bucket_name=store_bucket)
    return store_uri


@with_gcs_client
def gcs_transform_and_store(gcs_input, schema_config, source_type=None,
                            new_dir=None, new_file_name=None, 
                            data_root=None, metadata_root=None,
                            delimiter=None, fieldnames=None,
                            client=None, project_id=None,
                            source_bucket_override=None, store_bucket_override=None):

    '''
    Reads data from GCS (JSON or CSV/TXT), applies transform, 
    then writes transformed JSON back to GCS.
    Returns new GCS URI for downstream bulk load
    '''
    # Resolve Buckets
    source_bucket = resolve_bucket(override=source_bucket_override)
    store_bucket = resolve_bucket(purpose='tmp', override=store_bucket_override)

    logger.debug(f'gcs_transform_and_store: source_type={source_type}')

    if not schema_config:
        raise ValueError('Schema must be provided via schema_config or table_config')
    
    # Parse GCS input to get the bucket and the list of paths
    _, paths, _ = parse_gcs_input(gcs_input, client=client, project_id=project_id, bucket_name=source_bucket)

    new_uris = []
    # Transform and store for each path
    for path in paths:
        logger.debug(f'source_type={source_type}')
        new_uri = gcs_transform_and_store_file(path, schema_config, source_type=source_type, new_dir=new_dir, new_file_name=new_file_name,
                                           data_root=data_root, metadata_root=metadata_root,
                                           delimiter=delimiter, fieldnames=fieldnames,
                                           client=client, project_id=project_id,
                                           source_bucket_override=source_bucket,
                                           store_bucket_override=store_bucket)
        
        new_uris.append(new_uri)

    return new_uris


def transform_csv_records(content, schema_config, context_values=None, 
                          metadata=None, delimiter=None, fieldnames=None):
    metadata = metadata or {}
    logger.debug(f'transform_csv_records: Beginning transform of data={content[:100]}')
    delimiter = delimiter or ','
    reader = csv.DictReader(StringIO(content), fieldnames=fieldnames, delimiter=delimiter)
    csv_data = list(reader)
    logger.trace(f'transform_csv_records: csv_data sample={csv_data[:2]}')

    rows = [transform_record(record, schema_config, 
                             context_values=context_values, 
                             metadata=metadata) for record in csv_data]
    logger.trace(f'transform_csv_records: transformed rows sample={rows[:2]}')
    return rows

def transform_json_records(content, schema_config, context_values=None,
                           metadata=None, json_root=None):
    metadata = metadata or {}
    logger.debug(f'transform_json_records: Beginning transform of data={content[:100]}')
    logger.trace(f'transform_json_records: json_root={json_root}')
    json_data = json.loads(content)
    logger.trace(f'transform_json_records: json_data={json_data}')
    # Drill down if json_root is provided
    if json_root:
        for key in json_root:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f'Key {key} not found in JSON data')
    logger.trace(f'transform_json_records: Drilled down json_data={json_data}')
    # At this point, json_data could be a dict (single record) or list (multiple records)
    if not isinstance(json_data, list):
        json_data = [json_data]
    rows = [transform_record(record, schema_config, 
                             context_values=context_values,
                             metadata=metadata) for record in json_data]
    logger.trace(f'transform_json_records: transformed rows sample = {rows[:2]}')
    return rows


