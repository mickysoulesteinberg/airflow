CONFIG_SECTIONS = {
    'storage_config': {
        'gcs_bucket': 'bucket',
        'gcs_path': 'path',
    },
    'data_config': {
        'source_type': 'type',
        'source_delimiter': 'delimiter',
        'source_field_names': 'fields',
        'data_root': 'root',
    },
    'bigquery_config': {
        'bigquery_dataset': 'dataset',
        'bigquery_table': 'table',
        'bigquery_schema': 'schema'
    },
    'api_config': {
        'api_name': 'name',
        'api_arg_fields': 'arg_fields',
        'api_arg_builder': 'arg_builder',
        'api_path': 'path'
    }
}

# constants used elsewhere
BQ_METADATA_COL = 'airflow_metadata'
BQ_TIMESTAMP_COL = 'last_updated'
