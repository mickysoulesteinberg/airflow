from airflow.decorators import task
from core.gcs import load_json_from_gcs
from core.bq import insert_row_json
from transforms.helpers import transform_record

@task
def transform_and_insert(schema_config, table_id, gcs_path, project_id=None):
    '''
    Fetch JSON from GCS, transform into a schema-aligned row, insert into BigQuery.
    '''
    json_data = load_json_from_gcs(gcs_path, project_id)
    row = transform_record(json_data, schema_config, gcs_path)
    return insert_row_json(table_id, row, project_id)
