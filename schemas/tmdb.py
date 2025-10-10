MOVIES_SCHEMA = {
    'schema': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'title', 'type': 'STRING'},
        {'name': 'release_date', 'type': 'DATE'},
        {'name': 'popularity', 'type': 'FLOAT'},
        {'name': 'vote_average', 'type': 'FLOAT'},
        {'name': 'vote_count', 'type': 'INTEGER'},

        {'name': 'gcs_uri', 'type': 'STRING', 'static': True},          # comes from task param
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'generated': 'now'} # auto timestamp
    ],
    'source_type': 'json',
    'partition': {'type': 'DAY', 'field': 'last_updated'},
    'clustering': ['id'],
    'row_id': ['id']
}

CREDITS_SCHEMA = {
    'schema': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'cast', 'type': 'JSON'},
        {'name': 'crew', 'type': 'JSON'},
        {'name': 'gcs_uri', 'type': 'STRING', 'static': True},
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'generated': 'now'}
    ],
    'source_type': 'json',
    'partition': {'type': 'DAY', 'field': 'last_updated'},
    'clustering': ['id'],
    'row_id': ['id']
}

TABLES = {
    'tmdb.discover_movies': MOVIES_SCHEMA,
    'tmdb.credits': CREDITS_SCHEMA
}
