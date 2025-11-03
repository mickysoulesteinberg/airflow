ARTISTS_RAW = {
    'api_path': 'artists',
    'raw_schema': {
        'id': 'STRING',
        'name': 'STRING',
        'type': 'STRING',
        'uri': 'STRING',
        'followers': {
            'type': 'INTEGER', 
            'source': 'followers.total'
        },
        'popularity': 'INTEGER',
        'external_urls': 'JSON'
    },
    'row_id': ['id'],
    'arg_fields': {
        'artist': {
            'path_var': 'artist_id'
        }
    }
}