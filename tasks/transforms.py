from airflow.decorators import task
from core.gcs import load_json_from_gcs
from core.bq import insert_row_json, bq_current_timestamp, transform_record

@task
def transform_and_insert(schema_config, table_id, gcs_path, project_id=None, json_root=None):
    '''
    Fetch JSON from GCS, optionally unwrap with json_root, transform into schema-aligned rows, 
    and insert into BigQuery.
    '''
    json_data = load_json_from_gcs(gcs_path, project_id)
    
    # Define context values to use for static/generated columns
    context_values = {
        'gcs_uri': gcs_path,
        'last_updated': bq_current_timestamp()
    }

    # Drill down if json_root is provided
    if json_root:
        for key in json_root:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f"json_root step '{key}' not found in JSON at {gcs_path}")

    # At this point, json_data could be a dict (single record) or list (multiple records)
    if isinstance(json_data, list):
        rows = [transform_record(record, schema_config, context_values) for record in json_data]
    else:
        rows = [transform_record(json_data, schema_config, context_values)]

    # Insert all rows
    results = []
    for row in rows:
        result = insert_row_json(table_id, row, project_id)
        results.append(result)

    return results
