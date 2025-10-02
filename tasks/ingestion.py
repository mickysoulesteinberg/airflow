from airflow.decorators import task
import logging, os, jmespath
from core.api import api_get
from utils.helpers import get_valid_kwargs
from core.gcs import upload_to_gcs
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)


from airflow.decorators import task
from airflow.operators.python import get_current_context
from core.storage_utils import compute_storage_metadata
import os

@task
def get_storage_data(api, api_path, call_params=None):
    context = get_current_context()
    dag_id = context['dag'].dag_id
    ds_nodash = context['ds_nodash']

    gcs_path, staging, final = compute_storage_metadata(
        api = api,
        api_path = api_path,
        job_id = dag_id,
        run_suffix = ds_nodash,
        call_params = call_params
    )

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

