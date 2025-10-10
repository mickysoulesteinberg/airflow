NAMES_SCHEMA = {
    'schema': [
        {'name': 'year', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'sex', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'num_births', 'type': 'INTEGER', 'mode': 'REQUIRED'}
    ],
    'source_type': 'csv',
    'partition': 'year',
    'row_id': ['year', 'name', 'sex']
}

# NAMES_BY_STATE_SCHEMA = {

# }

# NAMES_BY_TERRITORY_SCHEMA = {

# }
