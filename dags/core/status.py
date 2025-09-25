from google.cloud import bigquery
import os, logging
from utils.config import CONFIG

logger = logging.getLogger(__name__)
PROJECT = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project=PROJECT)
GCS_BUCKET = CONFIG['gcs_bucket']

def update_status_table(dag_id, **kwargs):
    print(f'Updating Status table for {dag_id}. (This function is in progress)')