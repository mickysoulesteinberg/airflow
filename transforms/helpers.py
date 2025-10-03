from datetime import datetime

def transform_record(json_data: dict, schema_config: list, gcs_path: str = None) -> dict:
    '''
    Build a row dict for BigQuery from JSON data based on schema_config.
    - `json_path`: field name in JSON
    - `static`: fill with gcs_path
    - `generated`: supports 'now' for current timestamp
    '''
    row = {}

    for col in schema_config:
        name = col['name']

        if 'json_path' in col:
            # Pull directly from JSON
            value = json_data.get(col['json_path'])
            row[name] = value

        elif col.get('static'):
            # Inject GCS path
            row[name] = gcs_path

        elif col.get('generated') == 'now':
            # Insert timestamp in ISO format
            row[name] = datetime.utcnow().isoformat()

        else:
            # Default: None if no mapping rule
            row[name] = None

    return row
