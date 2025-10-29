SSA_NAMES_RAW = {
    'raw_schema': {
        'name': 'STRING',
        'sex': 'STRING',
        'num_births': 'INTEGER'
    },
    'row_id': ['name', 'sex'],
    'source_type': 'csv',
    'gcs_bucket': 'ssa_data_bucket',
    'gcs_path': 'names/yob*.txt',
    'bigquery_table': 'names',
    'context_fields': {'FILE_NAME': {'row_id': True}}
}