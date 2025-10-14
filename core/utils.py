
def resolve_gcs_uri(gcs_input, bucket_name=None):
    '''
    Ensures a string is in GCS URI format(gs://bucket/path)
    If gcs_input is a path, must pass a bucket name separately
    '''
    if not isinstance(gcs_input, str):
        raise TypeError('gcs_input must be a string')
    gcs_input = gcs_input.strip()
    if gcs_input.startswith('gs://'):
        return gcs_input
    if not bucket_name:
        raise ValueError('If gcs_input is not a GCS URI, bucket_name must be provided')
    return f'gs://{bucket_name}/{gcs_input.lstrip("/")}'

def split_gcs_uri(uri):
    '''Return (bucket, path) tuple from a GCS URI'''
    if not uri.startswith('gs://'):
        raise ValueError(f'Invalid GCS URI: {uri}')
    _, _, remainder = uri.partition('gs://')
    bucket, _, path = remainder.partition('/')
    return bucket, path

