NAMES_SCHEMA = {
    'schema': [
        # Data fields
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'sex', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'num_births', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        # Generated / Static fields
        {'name': 'gcs_uri', 'type': 'STRING', 'static': True},
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'generated': True}
    ],
    'source_type': 'csv',
    'partition': 'last_updated',
    'fieldnames': ['name', 'sex', 'num_births'],
    'delimiter': ',',
    'row_id': ['name', 'sex'],
    'merge_cols': ['name', 'sex', 'gcs_uri']
}

# NAMES_BY_STATE_SCHEMA = {

# }

# NAMES_BY_TERRITORY_SCHEMA = {

# }
