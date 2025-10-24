from airflow.decorators import task
from airflow.operators.python import get_current_context
import jmespath
from core.api import api_get
from pipeline.ingest import upload_json_to_gcs_og, upload_json_to_gcs
from pipeline.dag_helpers import create_gcs_prefix, create_gcs_file_name
from core.utils import join_gcs_path
from utils.helpers import get_valid_kwargs

from config.logger import get_logger
logger = get_logger(__name__)

@task(multiple_outputs=True)
def setup_etl(**kwargs):
    return_dict = {}

    api = kwargs.get('api')
    api_path = kwargs.get('api_path')
    if api and api_path:
        context = get_current_context()
        dag_id = context['dag'].dag_id
        return_dict['gcs_prefix'] = create_gcs_prefix(dag_id, api, api_path)

    return return_dict


@task(multiple_outputs=True)
def setup_api_call(config=None, **kwargs):
    
    # Get input fields
    config = config or {}
    api_arg_builder = kwargs.get('api_arg_builder') or config.get('api_arg_builder') or {}
    api_arg_fields = kwargs.get('arg_fields') or config.get('arg_fields') or {}
    logger.micro(f'api_arg_fields={api_arg_fields}')

    # Filter kwargs to those used in api_arg_builder
    api_call_kwargs = {f: kwargs[f] for f in api_arg_fields if f in kwargs}

    # Get api arg dict
    logger.verbose(f'Setting up API call with api_args={api_call_kwargs}')
    api_args = api_arg_builder(**api_call_kwargs)

    # Create gcs file name
    context = get_current_context()
    suffix = context['ds_nodash']
    gcs_file_name = create_gcs_file_name(api_call_kwargs, suffix=suffix)

    return {'gcs_file_name': gcs_file_name, 'api_args': api_args, 'metadata': api_call_kwargs}

@task(multiple_outputs=True)
def api_fetch_and_load(api, api_path, api_args, 
                       gcs_path=None, gcs_prefix=None, gcs_file_name=None,
                       bucket_override=None,
                       return_data=None,
                       metadata=None):


    logger.info(f'api_fetch_and_load: Beginning task for call to {api}.{api_path}')
    
    # Perform API call
    filtered_api_args = get_valid_kwargs(api_get, api_args)
    logger.debug(f'api_fetch_and_load: api_args={filtered_api_args}')
    data = api_get(api, path=api_path, **filtered_api_args)

    return_dict = {}

    # Build GCS path if necessary
    logger.debug(f'gcs_path={gcs_path}, gcs_prefix={gcs_prefix}, gcs_file_name={gcs_file_name}')
    if gcs_path is None and gcs_prefix and gcs_file_name:
        gcs_path = join_gcs_path(gcs_prefix, gcs_file_name)

    # Upload to GCS if path provided
    if gcs_path:
        upload_json_to_gcs(data, gcs_path, metadata=metadata, bucket_override=bucket_override)
        return_dict['gcs_path'] = gcs_path

    # Get data to return if requested
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)

    return return_dict


@task(multiple_outputs=True)
def api_fetch_and_load_og(api=None, api_path=None, api_args=None, 
                       gcs_path=None, gcs_prefix=None, gcs_file_name=None,
                       bucket_override=None,
                       return_data=None, api_call_dict=None):
    """
    Fetch data from an API and optionally upload to GCS.
    Can be called in two ways:
      1. api_fetch(api='tmdb', api_path='discover_movies', api_args={...})
      2. api_fetch(api_call_dict={'api': ..., 'api_path': ..., 'api_args': ...})
    """

    if api_call_dict:
        # merge values from dict into function vars if not passed explicitly
        api = api or api_call_dict.get('api')
        api_path = api_path or api_call_dict.get('api_path')
        api_args = api_args or api_call_dict.get('api_args')
        gcs_prefix = gcs_prefix or api_call_dict.get('gcs_prefix')
        gcs_file_name = gcs_file_name or api_call_dict.get('gcs_file_name')
        gcs_path = gcs_path or api_call_dict.get('gcs_path')
        return_data = return_data or api_call_dict.get('return_data')

    if not api or not api_path or not api_args:
        raise ValueError("api, api_path, and api_args are required either directly or via api_call_dict.")

    # Perform API call
    data = api_get(api, path=api_path, **api_args)

    return_dict = {}

    # Build GCS path if necessary
    if gcs_path is None and gcs_prefix and gcs_file_name:
        gcs_path = join_gcs_path(gcs_prefix, gcs_file_name)

    # Upload to GCS if path provided
    if gcs_path:
        upload_json_to_gcs_og(data, gcs_path, bucket_override=bucket_override)
        return_dict['gcs_path'] = gcs_path

    # Get data to return if requested
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)

    return return_dict
