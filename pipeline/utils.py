from core.gcs import list_gcs_files, with_gcs_client
from core.utils import extract_gcs_prefix, resolve_gcs_file, resolve_gcs_path, looks_like_file
import logging
import fnmatch

logger = logging.getLogger(__name__)

@with_gcs_client
def parse_gcs_input_str(input_str, client=None, project_id=None, bucket_name=None):
    '''Parses a GCS input string (path, prefix, wildcard) and returns a list of uris or paths'''

    path_str, uri_str, bucket_name = resolve_gcs_file(input_str, bucket_name=bucket_name)
    logger.debug(f'Resolved gcs_input to: bucket_name={bucket_name}, path_str={path_str}, gcs_uri={uri_str}')

    # If input is a single file, we're done
    if looks_like_file(path_str) and '*' not in uri_str:
        return [uri_str], [path_str], bucket_name
    
    # Get prefix and list all files under it
    prefix = extract_gcs_prefix(path_str)
    logger.debug(f'Prefix for listing: {prefix}')
    uri_list = list_gcs_files(prefix, client=client, project_id=project_id, bucket_name=bucket_name)
    logger.debug(f'All files under prefix: {uri_list}')

    # Filter files to matching for wildcard
    if '*' in uri_str:
        uri_list = [
            u for u in uri_list
            if fnmatch.fnmatch(u, uri_str)
        ]

    path_list = [resolve_gcs_path(u) for u in uri_list]
    logger.debug(f'path_list: {path_list}')
    return uri_list, path_list, bucket_name


@with_gcs_client
def parse_gcs_input(gcs_input, client=None, project_id=None, bucket_name=None, multiple_buckets=False):
    '''
    Parses GCS input (string or list of strings) and returns:
    - if multiple_buckets=False -> (uri_list, path_list, bucket_name)
    - if multiple_buckets=True  -> {bucket_name: {'uri_list': [], 'path_list': []}}
    '''


    if bucket_name and multiple_buckets:
        logger.warning('bucket_name is provided but multiple_buckets is set to True.')

    if isinstance(gcs_input, str):
        gcs_input = [gcs_input]
    elif not isinstance(gcs_input, list):
        raise TypeError('gcs_input must be a string or a list of strings')

    buckets = {}

    for input_str in gcs_input:
        uri_list, path_list, this_bucket = parse_gcs_input_str(input_str,
                                                               client=client, project_id=project_id,
                                                               bucket_name=bucket_name)

        if this_bucket not in buckets:
            buckets[this_bucket] = {'uri_list': [], 'path_list': []}

        buckets[this_bucket]['uri_list'].extend(uri_list)
        buckets[this_bucket]['path_list'].extend(path_list)

    # Error if more than one bucket and multiple_buckets=False
    if not multiple_buckets and len(buckets) > 1:
        raise ValueError('gcs_input resulted in multiple buckets. Set multiple_buckets=True to allow.')

    # Remove duplicates
    for _, gcs_output in buckets.items():
        # Sort and dedupe zipped object to ensure consistent output and aligned lists
        paired = list(zip(gcs_output['uri_list'], gcs_output['path_list']))
        deduped_sorted = sorted(set(paired))
        uri_list, path_list = zip(*deduped_sorted) if deduped_sorted else ([], [])

        gcs_output['uri_list'] = uri_list
        gcs_output['path_list'] = path_list
    
    if not multiple_buckets:
        bucket_name = next(iter(buckets))
        gcs_output = buckets[bucket_name]
        return gcs_output['uri_list'], gcs_output['path_list'], bucket_name
    


    # If multiple_buckets=True, return the full dictionary
    return buckets
