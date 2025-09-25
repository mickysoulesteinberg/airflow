from airflow.decorators import task_group
import logging
from tasks.ingestion import api_fetch, transform_data, load_data

logger = logging.getLogger(__name__)

# will move this somewhere else
def default_bucket_and_paths(api, dag_id):
    # from utils.config import FULL_CONFIG
    # api_config = FULL_CONFIG[api] #maybe use to get path?
    # gcs_bucket = 'TODO: GCS_BUCKET'
    # # Should optionally include task group id or task id in case of the same api call
    # return {
    #     'bucket': gcs_bucket,
    #     'base_path': f'{gcs_bucket}/{dag_id}/{api}/params'
    # }
    return {'bucket': 'test-bucket', 'base_path': 'test-base-path'}

@task_group
def api_fetch_and_load(
    api, # name of api, required
    dag_id, # required
    api_path,
    api_path_vars = None,
    api_params = None,
    api_token_data = None,
    # save_to_gcs = True, # Save raw json data to gcs
    gcs_bucket = None

):
    # Get bucket and path to store the data
    gcs_config = gcs_bucket or default_bucket_and_paths(api, dag_id)
    gcs_bucket = gcs_config['bucket']
    gcs_path_prefix = gcs_config['base_path']

    # Get the API data and load to GCS or BigQuery
    api_fetch(api, path = api_path, path_vars = api_path_vars, params = api_params, token_data = api_token_data) 

    
    # transform_data() # get data from gcs. Path inherited from api_fetch
    # load_data() #load data to bigquery
    # Also want to update status table as we go? using core.status