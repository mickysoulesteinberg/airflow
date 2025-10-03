from airflow.decorators import task
from core.gcs import load_json_from_gcs
from core.bq import insert_row_json
from transforms.helpers import transform_record

# @task
# def transform_and_insert(schema_config, table_id, gcs_path, project_id=None):
#     '''
#     Fetch JSON from GCS, transform into a schema-aligned row, insert into BigQuery.
#     '''
#     json_data = load_json_from_gcs(gcs_path, project_id)
#     row = transform_record(json_data, schema_config, gcs_path)
#     return insert_row_json(table_id, row, project_id)


@task
def transform_and_insert(schema_config, table_id, gcs_path, project_id=None, json_path=None):
    '''
    Fetch JSON from GCS, optionally unwrap with json_path, transform into schema-aligned rows, 
    and insert into BigQuery.
    '''
    json_data = load_json_from_gcs(gcs_path, project_id)

    # Drill down if json_path is provided
    if json_path:
        for key in json_path:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f"json_path step '{key}' not found in JSON at {gcs_path}")

    # At this point, json_data could be a dict (single record) or list (multiple records)
    if isinstance(json_data, list):
        rows = [transform_record(record, schema_config, gcs_path) for record in json_data]
    else:
        rows = [transform_record(json_data, schema_config, gcs_path)]

    # Insert all rows
    results = []
    for row in rows:
        result = insert_row_json(table_id, row, project_id)
        results.append(result)

    return results
