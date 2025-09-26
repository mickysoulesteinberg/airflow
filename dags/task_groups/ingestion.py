from airflow.decorators import task_group
from tasks.ingestion import api_fetch, transform_data, load_data, get_storage_data

@task_group
def api_fetch_and_load(
    api, 
    api_path,
    api_args = None
):

    gcs_path = get_storage_data(
        api,
        api_path,
        api_args
    )

    api_fetch(
        api,
        api_path,
        api_args = api_args,
        gcs_path = gcs_path,
    )

    # transform_data() # get data from gcs. Path inherited from api_fetch
    # load_data() #load data to bigquery
    # Also want to update status table as we go? using core.status