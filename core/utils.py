from core.env import resolve_bucket
import logging

logger = logging.getLogger(__name__)

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
        bucket_name = resolve_bucket(bucket_name)
        gcs_path = gcs_input
        gcs_uri = f'gs://{bucket_name}/{gcs_path.lstrip("/")}'
    return gcs_path, gcs_uri, bucket_name

def resolve_gcs_uri(gcs_input, bucket_name=None):
    '''Takes a path or URI and returns a GCS URI'''
    _, gcs_uri, _ = resolve_gcs_file(gcs_input, bucket_name=bucket_name)
    return gcs_uri

def resolve_gcs_path(gcs_input):
    if gcs_input.startswith('gs://'):
        gcs_path, _, _ = resolve_gcs_file(gcs_input)
        return gcs_path
    else:
        return gcs_input
    
def extract_gcs_prefix(input_str):
    '''
    Returns the (fixed) prefix portion of a GCS path pattern.
    E.g. 
        'folder/subfolder/*.json' -> 'folder/subfolder'
        'folder/subfolder/file.json' -> 'folder/subfolder'
        'folder/*/subfolder/' -> 'folder'
    '''
    path_str = resolve_gcs_path(input_str)

    if '*' in path_str:
        # Wildcard present, return portion before first wildcard
        path_str = path_str.split('*')[0]
    if '/' in path_str:
        return path_str.rsplit('/', 1)[0] + '/'
    return ''
    
