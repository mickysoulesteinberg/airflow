from airflow.decorators import task
import logging, os
from core.api import api_get
from utils.helpers import get_valid_kwargs
from core.gcs import upload_to_gcs
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)

@task
def get_storage_data(api, api_path, api_args):
    context = get_current_context()
    dag_id = context['dag'].dag_id
    ds_nodash = context['ds_nodash']
    api_call_id = api_args.get('call_id')
    gcs_file_name = f'{api_call_id}-{ds_nodash}.json'
    gcs_path = f'{dag_id}/{api}/{api_path}/{gcs_file_name}'
    return gcs_path

@task
def api_fetch(api, api_path, api_args, gcs_path = None):
    '''
    Fetch Data from API. Optionally dump raw JSON to GCS (else returns data)

    Args:
        api (str): API name (e.g. 'tmdb', 'spotify')
        api_path (str): API path key (e.g. 'discover_movies')
        gcs_path (str | None | False):
            - str: use this explicit GCS path and upload
            - None: skip upload, return raw json
    
    Returns:
        dict: {
            'data': dict | list | None,
            'gcs_url': str | None
        }
    '''    

    # Get API data as JSON
    data = api_get(api, path = api_path, **get_valid_kwargs(api_get, api_args))

    # If gcs_path is explicitly False, just return the raw data
    if gcs_path is None:
        return {'data': data, 'gcs_uri': None}
    
    # Upload the data to the GCS Bucket and return the uri
    uri = upload_to_gcs(gcs_path, data)
    logger.warning(f'Created uri: {uri}')
    return {'data': None, 'gcs_uri': uri}

@task
def transform_data():
    logger.warning('transform_data: Transforming raw JSON api data to load into BigQuery')

@task
def load_data():
    logger.warning('load_data:Loading transformed data into BigQuery')