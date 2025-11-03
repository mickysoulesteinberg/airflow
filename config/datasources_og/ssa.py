GCS_BUCKET = 'ssa_data_bucket'

SSA_NAMES_RAW = {
    'raw_schema': {
        'name': 'STRING',
        'sex': 'STRING',
        'num_births': 'INTEGER'
    },
    'row_id': ['name', 'sex'],
    'source_type': 'csv',
    'gcs_bucket': GCS_BUCKET,
    'gcs_path': 'names/yob*.txt',
    'bigquery_table': 'names',
    'context_fields': {'FILE_NAME': {'row_id': True}}
}

SSA_STATE_NAMES_RAW = {
    'raw_schema': {
        'state': 'STRING',
        'sex': 'STRING',
        'year': 'INTEGER',
        'name': 'STRING',
        'num_births': 'INTEGER'
    },
    'row_id': ['state', 'sex', 'year', 'name'],
    'source_type': 'csv',
    'gcs_bucket': GCS_BUCKET,
    'gcs_path': 'namesbystate/M*.TXT',
    'bigquery_table': 'state_names'
}

SSA_TERRITORY_NAMES_RAW = {
    'raw_schema': {
        'territory': 'STRING',
        'sex': 'STRING',
        'year': 'INTEGER',
        'name': 'STRING',
        'num_births': 'INTEGER'
    },
    'row_id': ['state', 'sex', 'year', 'name'],
    'source_type': 'csv',
    'gcs_bucket': GCS_BUCKET,
    'gcs_path': 'namesbyterritory/*.TXT',
    'bigquery_table': 'territory_names'
}