from airflow.decorators import task
import logging, jmespath
from core.api import api_get
from core.gcs import upload_json_to_gcs


logger = logging.getLogger(__name__)


@task(multiple_outputs=True)
def api_fetch_and_load(api=None, api_path=None, api_args=None, gcs_path=None,
              return_data=None, api_call_dict=None):
    """
    Fetch data from an API and optionally upload to GCS.
    Can be called in two ways:
      1. api_fetch(api='tmdb', api_path='discover_movies', api_args={...})
      2. api_fetch(api_call_dict={'api': ..., 'api_path': ..., 'api_args': ...})
    """

    # ---- Allow dict-style input for flexibility ----
    if api_call_dict:
        # merge values from dict into function vars if not passed explicitly
        api = api or api_call_dict.get('api')
        api_path = api_path or api_call_dict.get('api_path')
        api_args = api_args or api_call_dict.get('api_args')
        gcs_path = gcs_path or api_call_dict.get('gcs_path')
        return_data = return_data or api_call_dict.get('return_data')

    # ---- Safety check ----
    if not api or not api_path or not api_args:
        raise ValueError("api, api_path, and api_args are required either directly or via api_call_dict.")

    # ---- Initialize result ----
    return_dict = {}

    # ---- Fetch data ----
    data = api_get(api, path=api_path, **api_args)

    # ---- Upload to GCS ----
    if gcs_path:
        upload_json_to_gcs(data, gcs_path)
        return_dict['gcs_path'] = gcs_path

    # ---- Extract return_data ----
    if return_data:
        for key, expr in return_data.items():
            return_dict[key] = jmespath.search(expr, data)

    return return_dict
