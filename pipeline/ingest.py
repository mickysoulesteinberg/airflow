from core.gcs import logger, with_bucket, with_gcs_client, upload_from_string
from core.utils import resolve_gcs_uri
import json
from core.env import resolve_bucket
from config.datasources import BQ_METADATA_COL, RAW_DATA_KEY


def get_default_bucket(bucket_override):
    '''Returns the bucket name to use for ingestion tasks'''
    logger.trace(f'get_default_bucket: bucket_override={bucket_override}')
    bucket_name = resolve_bucket(override=bucket_override)
    return bucket_name

@with_gcs_client
def upload_json_to_gcs(data, path, metadata=None, new_line=False,
                       client=None, project_id=None,
                       bucket_override=None):
    
    # Resolve bucket and uri
    bucket_name = get_default_bucket(bucket_override)
    uri = resolve_gcs_uri(path, bucket_name = bucket_name)

    logger.debug(f'upload_json_to_gcs: Beginning upload to {uri}')

    # Wrap with metadata if necessary
    if metadata:
        logger.debug(f'upload_json_gcs: Wrapping JSON data with metadata={metadata}')
        upload_data = {
            BQ_METADATA_COL: metadata,
            RAW_DATA_KEY: data
        }
    else:
        upload_data = data

    if new_line:
        logger.trace('upload_json_gcs: new_line=True')
        data_string = '\n'.join(json.dumps(r) for r in upload_data)
    else:
        data_string = json.dumps(upload_data)

    upload_from_string(data_string, path,
                       client=client, project_id=project_id, bucket_name=bucket_name)

    logger.verbose(f'Wrote file: {uri}')

    return uri


@with_gcs_client
def upload_json_to_gcs_og(data, path, wrap=True, new_line=False,
                       client=None, project_id=None,
                       bucket_override=None):
    bucket_name = get_default_bucket(bucket_override)
    uri = resolve_gcs_uri(path, bucket_name = bucket_name)
    if wrap:
        # TODO don't need this wrap logic, and don't need wrapped data. Must change schema before removing.
        data = {'uri': uri, 'data': data}
    data_string = ''
    if new_line:
        data_string = '\n'.join(json.dumps(r) for r in data)
    else:
        data_string = json.dumps(data)

    upload_from_string(data_string, path, client=client, project_id=project_id, bucket_name=bucket_name)

    logger.info(f'Wrote file: {uri}')

    return uri

