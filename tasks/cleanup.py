from airflow.decorators import task
from core.gcs import delete_gcs_folder
from pipeline_utils.cleanup import delete_gcs_files

@task
def delete_gcs_tmp_folder(tmp_folder_path, wait_for=None):
    '''
    Deletes a GCS folder and all its contents. 
    Use wait_for to ensure this task runs after other tasks.
    '''
    delete_gcs_folder(tmp_folder_path)
    return

@task
def delete_gcs_tmp_files(gcs_input, wait_for=None):
    '''
    Deletes GCS files.
    gcs_input can be a single path, a list of paths, a prefix, or a wildcard pattern.
    Use wait_for to ensure this task runs after other tasks.
    '''
    delete_gcs_files(gcs_input)
    return

@task
def delete_bq_staging_table(staging_table, wait_for=None):
    '''
    Deletes a BigQuery staging table. 
    Use wait_for to ensure this task runs after other tasks.
    '''
    from core.bq import delete_table
    delete_table(staging_table)
    return