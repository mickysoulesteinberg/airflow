# core/storage_utils.py (helper, pure Python)
from airflow.operators.python import get_current_context

def compute_storage_metadata(
        api,
        api_path,
        job_id = 'not_a_dag',
        run_suffix = 'data',
        call_params=None
    ):

    # GCS file name
    gcs_file_name = f'{run_suffix}.json'
    if call_params:
        call_id = '-'.join(f'{k}{v}' for k, v in call_params.items())
        gcs_file_name = f'{call_id}-{gcs_file_name}'

    gcs_path = f'{job_id}/{api}/{api_path}/{gcs_file_name}'

    # TODO Hardcoding specifically for load_top_movies_to_bigquery, will use configs later
    dataset = 'tmdb'
    base_table = 'discover_movies'
    bq_staging_table = f'{dataset}.{base_table}_stg'
    bq_final_table   = f'{dataset}.{base_table}'

    return gcs_path, bq_staging_table, bq_final_table
