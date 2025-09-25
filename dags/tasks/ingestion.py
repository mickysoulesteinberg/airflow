from airflow.decorators import task
import logging
from core.api import api_get


logger = logging.getLogger(__name__)

@task
def api_fetch(api, path, path_vars = None, params = None, token_data = None):
    
    # TODO
    logger.warning('api_fetch: Fetching data from api, loading raw data to gcs/bigquery')
    # Store data to bigquery. Return data or path to raw data.
    return api_get(api, path, path_vars = path_vars, params = params, token_data = token_data)

@task
def transform_data():
    logger.warning('transform_data: Transforming raw JSON api data to load into BigQuery')

@task
def load_data():
    logger.warning('load_data:Loading transformed data into BigQuery')