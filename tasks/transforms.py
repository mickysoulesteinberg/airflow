from airflow.decorators import task
from core.bq import transform_and_insert, transform_and_insert_json, transform_and_insert_json2

@task
def gcs_to_bq_stg(schema_config, dataset_table, gcs_path, project_id=None, json_root=None):
    
    # transform_and_insert(schema_config, dataset_table = dataset_table, gcs_path = gcs_path, project_id = project_id, json_root = json_root)
    # transform_and_insert_json(schema_config, dataset_table = dataset_table, gcs_path = gcs_path, project_id = project_id, json_root = json_root)
    transform_and_insert_json2(schema_config, dataset_table = dataset_table, gcs_path = gcs_path, project_id = project_id, json_root = json_root)

    return dataset_table

    

