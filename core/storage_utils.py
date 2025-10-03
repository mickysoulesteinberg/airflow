from airflow.operators.python import get_current_context

def compute_storage_metadata(
        api,
        api_path,
        top_folder = 'not_a_dag',
        run_suffix = 'data',
        call_params=None
    ):

    # GCS file name
    gcs_file_name = f'{run_suffix}.json'
    if call_params:
        call_id = '-'.join(f'{k}{v}' for k, v in call_params.items())
        gcs_file_name = f'{call_id}-{gcs_file_name}'

    gcs_path = f'{top_folder}/{api}/{api_path}/{gcs_file_name}'

    # TODO Hardcoding specifically for load_top_movies_to_bigquery, will use configs later
    dataset = 'tmdb'
    base_table = 'discover_movies'
    bq_staging_table = f'{dataset}.{base_table}_stg'
    bq_final_table   = f'{dataset}.{base_table}'

    return {'gcs_path': gcs_path, 'bq_staging_table': bq_staging_table, 'bq_final_table': bq_final_table}

# Helper function to use within a task to get storage path for api data
def get_gcs_path(top_folder, api, api_path, call_params = None, run_suffix = None):

    gcs_path = compute_storage_metadata(
        api = api,
        api_path = api_path,
        top_folder = top_folder,
        run_suffix = run_suffix,
        call_params = call_params
    )['gcs_path']

    return gcs_path