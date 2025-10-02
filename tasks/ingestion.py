from airflow.decorators import task
import logging, os, jmespath
from core.api import api_get
from utils.helpers import get_valid_kwargs
from core.gcs import upload_to_gcs
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)

@task
def get_storage_data(api, api_path, call_params = None):

    # Get context to use in file path
    context = get_current_context()
    ds_nodash = context['ds_nodash']

    # Define file name based on date (default)
    gcs_file_name = f'{ds_nodash}.json'

    # Append GCS file name from dynamic API parameters
    if call_params:
        call_id = '-'.join(f'{key}{value}' for key,value in call_params.items())
        gcs_file_name = f'{call_id}-{gcs_file_name}'

    # Get GCS File path from dag and api info
    dag_id = context['dag'].dag_id
    gcs_path = f'{dag_id}/{api}/{api_path}/{gcs_file_name}'

    return gcs_path

@task(multiple_outputs = True)
def api_fetch(api, api_path, api_args, gcs_path = None, return_data = None):

    # Initiate results dictionary to return
    return_dict = {}

    # Get API data as JSON
    # data = api_get(api, path = api_path, **get_valid_kwargs(api_get, api_args))
    data = api_get(api, path = api_path, **api_args)

    # If gcs_uri is specified, load the raw data directly to GCS
    if gcs_path:
        upload_to_gcs(path = gcs_path, data = data)
    
    # If return_data is specified, extract the data to return to XCom
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)
        
    return return_dict



@task
def transform_data():
    logger.warning('transform_data: Transforming raw JSON api data to load into BigQuery')

@task
def load_data():
    logger.warning('load_data:Loading transformed data into BigQuery')

