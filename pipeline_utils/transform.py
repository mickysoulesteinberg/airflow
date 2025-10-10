from core.gcs import load_json_from_gcs, upload_json_to_gcs
from core.utils import bq_current_timestamp
import os, json

def transform_record(record, schema_config, context_values):
    row = {}
    for col in schema_config:
        name = col['name']
        json_path = col.get('json_path')
        col_type = col.get('type')

        if json_path:
            value = record.get(json_path)
        elif name in context_values:
            value = context_values[name]
        else:
            value = None

        if col_type == 'JSON' and value is not None:
            value = json.dumps(value)

        row[name] = value
    return row


def transform_json_records(schema_config, gcs_path, json_root=None):
    json_data = load_json_from_gcs(gcs_path)

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

    return rows

 
def gcs_transform_and_store(schema_config, path,
                             new_dir=None, new_file_name=None,
                             json_root=None):
    '''
    Reads JSON from GCS, applies transform, then write transformed back to GCS.
    Returns new GCS URI for downstream bulk load
    '''

    # TODO check path goes to a json file
    
    # Transform the raw json to BigQuery-ready format
    transformed_records = transform_json_records(schema_config, path, json_root=json_root)

    # Write to new GCS location
    dir = '/'.join(path.split('/')[:-1])
    new_dir = new_dir or f'{dir}/tmp'
    new_file_name = new_file_name or os.path.basename(path).replace('.json', '_transformed.json')
    new_blob_path = f'{new_dir}/{new_file_name}'

    new_uri = upload_json_to_gcs(transformed_records, new_blob_path, wrap=False, new_line=True)

    return {
        'path': new_blob_path,
        'uri': new_uri
    }


