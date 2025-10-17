from core.gcs import logger, with_bucket, with_gcs_client, upload_from_string
from core.utils import resolve_gcs_uri
import json
from core.env import resolve_bucket


def get_default_bucket(bucket_override):
    '''Returns the bucket name to use for ingestion tasks'''
    logger.trace(f'get_default_bucket: bucket_override={bucket_override}')
    bucket_name = resolve_bucket(override=bucket_override)
    return bucket_name

@with_gcs_client
def upload_json_to_gcs(data, path, wrap=True, new_line=False,
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