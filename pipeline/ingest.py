from config.config_wrapper import with_config
from utils.helpers import get_valid_kwargs
from core.gcs import logger, with_gcs_client, upload_from_string
from core.utils import resolve_gcs_uri
from core.api import api_get
import json
from core.env import resolve_bucket
from config.datasources import BQ_METADATA_COL, RAW_DATA_KEY
from pipeline.dag_helpers import create_gcs_prefix, create_gcs_file_name


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
def upload_json_to_gcs_og_og(data, path, wrap=True, new_line=False,
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

@with_config(['api_config'])
def resolve_api_arg_builder(config=None, *, api_arg_builder=None):
    return api_arg_builder

@with_config(['api_config'])
def resolve_api_arg_fields(config=None, *, api_arg_fields=None):
    return api_arg_fields

def get_api_args(config=None, **overrides):
    api_arg_builder = resolve_api_arg_builder(config=config, **overrides)
    api_arg_fields = resolve_api_arg_fields(config=config, **overrides)
    api_call_kwargs = {f: overrides[f] for f in api_arg_fields if f in overrides}
    api_args = api_arg_builder(**api_call_kwargs)
    filtered_api_args = get_valid_kwargs(api_get, api_args)
    return filtered_api_args


@with_config(['api_config'])
def api_fetch(config=None, *,
              # api_config
              api_name=None,
              api_path=None,
              #optional overrides
              api_args=None):
    
    logger.debug(f'api_fetch: api={api_name}, path={api_path}, api_args={api_args}')
    data = api_get(api_name, path=api_path, **api_args)
    return data
