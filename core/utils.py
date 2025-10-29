from core.env import resolve_default_bucket
import textwrap
from config.logger import get_logger
from config.datasources import BQ_TIMESTAMP_COL

logger = get_logger(__name__)

def looks_like_file(path_str):
    if path_str.endswith('/'):
        return False
    if '.' in path_str.split('/')[-1]:
        return True
    return False

def join_gcs_path(*parts, sep='/', force_file=False):
    '''
    Joins parts into a normalized GCS path or URI.
    
    Features:
      - Handles inputs like 'gs://bucket', 'folder/', '/subfolder', 'file.json'
      - Prevents duplicate slashes
      - Adds trailing '/' if it looks like a folder (no file extension)
      - Optional force_file=True disables trailing slash

    E.g.
        join_gcs_path('folder/', '/subfolder', 'file.ext') -> 'folder/subfolder/file.ext'
        join_gcs_path('data', '2025') -> 'data/2025/'
        join_gcs_path('gs://my-bucket', 'movies', '2025', 'data.json') -> 'gs://my-bucket/movies/2025/data.json'
    '''

    # Flatten and clean all parts
    cleaned = []
    for p in parts:
        if p:
            if not isinstance(p, str):
                logger.warning(f'Input non-string value {p}, skipping input.')
            else:
                part = p.strip(sep)
                cleaned.append(part)

    if not cleaned:
        return ''

    path = sep.join(cleaned).rstrip('/')

    # Add trailing slash if it looks like a folder and not forced as file
    if not force_file and '.' not in path.split(sep)[-1]:
        path += sep

    return path


def split_gcs_uri(uri):
    '''Return (bucket, path) tuple from a GCS URI'''
    if not uri.startswith('gs://'):
        raise ValueError(f'Invalid GCS URI: {uri}')
    _, _, remainder = uri.partition('gs://')
    bucket, _, path = remainder.partition('/')
    return bucket, path

def resolve_gcs_file(gcs_input, bucket_name=None):
    '''Helper to resolve GCS path or URI'''
    if not isinstance(gcs_input, str):
        raise TypeError('gcs_input must be a string')
    gcs_input = gcs_input.strip()

    gcs_uri = None
    gcs_path = None

    if gcs_input.startswith('gs://'):
        gcs_uri = gcs_input
        uri_bucket, gcs_path = split_gcs_uri(gcs_uri)
        if bucket_name and uri_bucket != bucket_name:
            raise ValueError(f'Bucket name in gcs_input ({uri_bucket}) does not match provided bucket_name ({bucket_name})')
        bucket_name = uri_bucket
    else:
        bucket_name = resolve_default_bucket(bucket_name)
        gcs_path = gcs_input
        gcs_uri = f'gs://{bucket_name}/{gcs_path.lstrip("/")}'
    return gcs_path, gcs_uri, bucket_name

def resolve_gcs_uri(gcs_input, bucket_name=None):
    '''Takes a path or URI and returns a GCS URI'''
    _, gcs_uri, _ = resolve_gcs_file(gcs_input, bucket_name=bucket_name)
    return gcs_uri

def resolve_gcs_path(gcs_input):
    if gcs_input.startswith('gs://'):
        resolved_path, _, _ = resolve_gcs_file(gcs_input)
    else:
        resolved_path = gcs_input
    logger.trace(f'resolve_gcs_path: gcs_input=%s, resolved_path=%s',
                 gcs_input, resolved_path)
    return resolved_path
    
def extract_gcs_prefix(input_str):
    '''
    Returns the (fixed) prefix portion of a GCS path pattern.
    E.g. 
        'folder/subfolder/*.json' -> 'folder/subfolder'
        'folder/subfolder/file.json' -> 'folder/subfolder'
        'folder/*/subfolder/' -> 'folder'
        'folder/subfolder/string*.json' -> 'folder/subfolder'
    '''
    path_str = resolve_gcs_path(input_str)

    if '*' in path_str:
        # Portion before first wildcard, trimmed to the last full directory
        before_star = path_str.split('*', 1)[0]
        prefix = before_star.rsplit('/', 1)[0] if '/' in before_star else ''
    elif looks_like_file(path_str):
        # Strip filename if it looks like a file
        prefix = path_str.rsplit('/', 1)[0] if '/' in path_str else ''
    else:
        # Already a directory; just normalize
        prefix = path_str.rstrip('/')

    logger.debug(f'extract_gcs_prefix: input string=%s, extracted prefix=%s',
                 input_str, prefix)
    return prefix

def extract_gcs_file_name(input_str):
    '''
    Returns the file name (with extension) of a GCS path pattern.
    '''
    prefix, _, file_name = input_str.rpartition('/')
    return file_name


def format_stage_merge_query(staging_table, final_table, schema, merge_cols):
    schema_cols = [col['name'] for col in schema]

    # Define clauses
    on_clause = ' AND '.join([f'F.`{col}` = S.`{col}`' for col in merge_cols])
    update_clause = ',\n    '.join([
        f'F.`{BQ_TIMESTAMP_COL}` = CURRENT_TIMESTAMP()' if col == BQ_TIMESTAMP_COL
        else f'F.`{col}` = S.`{col}`'
        for col in schema_cols
        if col not in merge_cols
    ])
    insert_cols = ', '.join([f'`{col}`' for col in schema_cols])
    values_clause = ', '.join([
        'CURRENT_TIMESTAMP()' if col == BQ_TIMESTAMP_COL
        else f'S.`{col}`'
        for col in schema_cols
    ])

    # Construct query
    query = f'''
    MERGE `{final_table}` F
    USING `{staging_table}` S
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols})
    VALUES ({values_clause});
    '''

    return textwrap.dedent(query).strip()
    

def resolve_bq_dataset_table(table=None, dataset=None, override_dataset=False):
    logger.micro(f'resolve_bq_dataset_table: Inputs: table={table}, dataset={dataset}')

    if '.' in table:
        dataset_part, table_part = table.split('.', 1)
        if override_dataset:
            resolved_dataset = dataset or dataset_part
        else:
            resolved_dataset = dataset_part or dataset
        resolved_table = table_part
        logger.trace(f'resolve_bq_dataset_table: Parsed dataset.table as {resolved_dataset}.{resolved_table}')
    else:
        resolved_dataset = dataset
        resolved_table = table

    full_name = f'{resolved_dataset}.{resolved_table}' if resolved_dataset else resolved_table
    logger.micro(f'resolve_bq_dataset_table result: {full_name}')

    return full_name, resolved_table, resolved_dataset


def collect_list(*args):
    logger.micro(f'collect_list_of_strings: args={args}')
    full_list = []
    for arg in args:
        if arg:
            if isinstance(arg, list):
                full_list += arg
            else:
                full_list.append(arg)
    logger.trace(f'collect_list_of_strings: concated_list={full_list}')
    return full_list