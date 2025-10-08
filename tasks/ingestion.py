from airflow.decorators import task
import logging, jmespath
from core.api import api_get
from core.gcs import upload_to_gcs
# from core.bq import create_table
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




@task(multiple_outputs=True)
def api_fetch(api=None, api_path=None, api_args=None, gcs_path=None,
              return_data=None, call_params=None, api_call_dict=None):
    """
    Fetch data from an API and optionally upload to GCS.
    Can be called in two ways:
      1. api_fetch(api='tmdb', api_path='discover_movies', api_args={...})
      2. api_fetch(api_call_dict={'api': ..., 'api_path': ..., 'api_args': ...})
    """

    # ---- Allow dict-style input for flexibility ----
    if api_call_dict:
        # merge values from dict into function vars if not passed explicitly
        api = api_call_dict.get('api', api)
        api_path = api_call_dict.get('api_path', api_path)
        api_args = api_call_dict.get('api_args', api_args)
        gcs_path = api_call_dict.get('gcs_path', gcs_path)
        return_data = api_call_dict.get('return_data', return_data)
        call_params = api_call_dict.get('call_params', call_params)

    # ---- Safety check ----
    if not api or not api_path or not api_args:
        raise ValueError("api, api_path, and api_args are required either directly or via api_call_dict.")

    # ---- Initialize result ----
    return_dict = {}

    # ---- Fetch data ----
    data = api_get(api, path=api_path, **api_args)

    # ---- Determine GCS path if not given ----
    if gcs_path is None:
        context = get_current_context()
        top_folder = context['dag'].dag_id
        run_suffix = context['ds_nodash']
        gcs_path = get_gcs_path(
            top_folder=top_folder,
            api=api,
            api_path=api_path,
            call_params=call_params,
            run_suffix=run_suffix
        )

    # ---- Upload to GCS ----
    if gcs_path:
        upload_to_gcs(path=gcs_path, data=data)
        return_dict['gcs_path'] = gcs_path

    # ---- Extract return_data ----
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)

    return return_dict




# @task(multiple_outputs = True)
# def api_fetch(api, api_path, api_args, gcs_path = None, return_data = None, call_params = None):

#     # Initiate results dictionary to return
#     return_dict = {}

#     # Get API data as JSON
#     # data = api_get(api, path = api_path, **get_valid_kwargs(api_get, api_args))
#     data = api_get(api, path = api_path, **api_args)

#     # Build the gcs path if it isn't specified (skip loading if gcs_path = False)
#     if gcs_path is None:
#         context = get_current_context()
#         top_folder = context['dag'].dag_id
#         run_suffix = context['ds_nodash']
#         gcs_path = get_gcs_path(top_folder = top_folder, api = api, api_path = api_path, call_params = call_params, run_suffix = run_suffix)
#     if gcs_path:
#         upload_to_gcs(path = gcs_path, data = data)
#         return_dict['gcs_path'] = gcs_path
    
#     # If return_data is specified, extract the data to return to XCom
#     if return_data:
#         for key, expr in return_data.items():
#             return_dict[key] = jmespath.search(expr, data)
        
#     return return_dict

