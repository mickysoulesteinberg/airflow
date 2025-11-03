from airflow.decorators import task
from airflow.operators.python import get_current_context
import jmespath
from pipeline.dag_helpers import create_gcs_prefix, create_gcs_file_name
from core.utils import join_gcs_path
from core.api import get_oauth2_token
import pipeline.ingest as pipeline

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

    if kwargs.get('get_token'):
        token_data = get_oauth2_token(api)
        return_dict['token_data'] = token_data

    return return_dict

@task(multiple_outputs=True)
def setup_api_call(config=None, **overrides):
    
    # Resolve api config
    api_arg_builder = pipeline.resolve_api_arg_builder(config=config, **overrides)
    api_arg_fields = pipeline.resolve_api_arg_fields(config=config, **overrides)

    # Get input call kwargs and build api_args
    api_call_kwargs = {f: overrides[f] for f in api_arg_fields if f in overrides}
    api_args = api_arg_builder(**api_call_kwargs)

    # Create gcs file name
    context = get_current_context()
    suffix = context['ds_nodash']
    gcs_file_name = create_gcs_file_name(api_call_kwargs, suffix=suffix)

    return {'gcs_file_name': gcs_file_name, 'api_args': api_args, 'api_call_kwargs': api_call_kwargs}


@task(multiple_outputs=True)
def api_fetch_and_load(gcs_file_name, gcs_prefix, *, config=None, return_data=None, metadata=None, **overrides):

    # Get API Args
    api_args = overrides.get('api_args') or pipeline.get_api_args(config=config, **overrides)
    logger.trace(f'api_args={api_args}')
    new_overrides = dict(overrides)
    new_overrides['api_args'] = api_args

    # Fetch data from API
    data = pipeline.api_fetch(config=config, **new_overrides)

    # Build storage path
    gcs_path = join_gcs_path(gcs_prefix, gcs_file_name)
    return_dict = {'gcs_path': gcs_path}
    logger.trace(f'Built storage path={gcs_path}')
    
    pipeline.upload_json_to_gcs(data=data, path=gcs_path, metadata=metadata)
    logger.trace(f'Uploaded to gcs')

    # Get data to return if requested
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)
    logger.trace(f'Built return Xcom data = {return_data}')
    return return_dict
