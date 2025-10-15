from airflow.decorators import task
from core.gcs import delete_gcs_folder


@task
def delete_gcs_tmp_folder(tmp_folder_path):
    delete_gcs_folder(tmp_folder_path)
    return