from airflow.decorators import task
import logging, jmespath
from core.api import api_get
from core.gcs import upload_to_gcs
from airflow.operators.python import get_current_context
from core.storage_utils import get_gcs_path


logger = logging.getLogger(__name__)


from core.storage_utils import compute_storage_metadata

@task
def get_storage_data(api, api_path, call_params=None):
    context = get_current_context()
    dag_id = context['dag'].dag_id
    ds_nodash = context['ds_nodash']

    gcs_path, staging, final = compute_storage_metadata(
        api = api,
        api_path = api_path,
        top_folder = dag_id,
        run_suffix = ds_nodash,
        call_params = call_params
    )

    return gcs_path


@task(multiple_outputs = True)
def api_fetch(api, api_path, api_args, gcs_path = None, return_data = None, call_params = None):

    # Initiate results dictionary to return
    return_dict = {}

    # Get API data as JSON
    # data = api_get(api, path = api_path, **get_valid_kwargs(api_get, api_args))
    data = api_get(api, path = api_path, **api_args)

    # Build the gcs path if it isn't specified (skip loading if gcs_path = False)
    if gcs_path is None:
        context = get_current_context()
        top_folder = context['dag'].dag_id
        run_suffix = context['ds_nodash']
        gcs_path = get_gcs_path(top_folder = top_folder, api = api, api_path = api_path, call_params = call_params, run_suffix = run_suffix)
    if gcs_path:
        upload_to_gcs(path = gcs_path, data = data)
        return_dict['gcs_path'] = gcs_path
    
    # If return_data is specified, extract the data to return to XCom
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)
        
    return return_dict

