from airflow.decorators import task
import logging, os, jmespath
from core.api import api_get
from utils.helpers import get_valid_kwargs
from core.gcs import upload_to_gcs
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)

@task
def get_storage_data(api, api_path, api_args):

    # Get context to use in file path
    context = get_current_context()

    # Get GCS file name from dynamic API parameters
    call_params = api_args.get('call_params', {})
    call_id = '-'.join(f'{key}{value}' for key,value in call_params.items())
    ds_nodash = context['ds_nodash']
    gcs_file_name = f'{call_id}-{ds_nodash}.json'.strip('-')

    # Get GCS File path from dag and api info
    dag_id = context['dag'].dag_id
    gcs_path = f'{dag_id}/{api}/{api_path}/{gcs_file_name}'

    return gcs_path

@task
def api_fetch(api, api_path, api_args, gcs_path = None, x_com_data = None):

    # Initiate results dictionary to return
    results = {}

    # Get API data as JSON
    data = api_get(api, path = api_path, **get_valid_kwargs(api_get, api_args))

    # If gcs_path is specified, load the raw data directly to GCS
    if gcs_path:
        gcs_uri = upload_to_gcs(gcs_path, data)
        results['gcs_uri'] = gcs_uri
    
    # If x_com_data is specified, extract the data to return to XCom
    if x_com_data:
        for key, expr in x_com_data.items():
            results[key] = jmespath.search(expr, data)
        
    return results



@task
def transform_data():
    logger.warning('transform_data: Transforming raw JSON api data to load into BigQuery')

@task
def load_data():
    logger.warning('load_data:Loading transformed data into BigQuery')