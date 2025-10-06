MOVIES_SCHEMA = {
    'schema': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED', 'json_path': 'id'},
        {'name': 'title', 'type': 'STRING', 'json_path': 'title'},
        {'name': 'release_date', 'type': 'DATE', 'json_path': 'release_date'},
        {'name': 'popularity', 'type': 'FLOAT', 'json_path': 'popularity'},
        {'name': 'vote_average', 'type': 'FLOAT', 'json_path': 'vote_average'},
        {'name': 'vote_count', 'type': 'INTEGER', 'json_path': 'vote_count'},

        {'name': 'gcs_uri', 'type': 'STRING', 'static': True},          # comes from task param
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'generated': 'now'} # auto timestamp
    ],
    'partition': {'type': 'DAY', 'field': 'last_updated'},
    'clustering': ['id']
}

CREDITS_SCHEMA = {
    'schema': [
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED', 'json_path': 'id'},
        {'name': 'cast', 'type': 'JSON', 'json_path': 'cast'},
        {'name': 'crew', 'type': 'JSON', 'json_path': 'crew'},
        {'name': 'gcs_uri', 'type': 'STRING', 'static': True},
        {'name': 'last_updated', 'type': 'TIMESTAMP', 'generated': 'now'}
    ],
    'partition': {'type': 'DAY', 'field': 'last_updated'},
    'clustering': ['id']
}

TABLES = {
    'tmdb.discover_movies': MOVIES_SCHEMA,
    'tmdb.credits': CREDITS_SCHEMA
}
