DISCOVER_MOVIES_RAW = {
    'api_path': 'discover_movies',
    'raw_schema': {
        'id': 'INTEGER',
        'title': 'STRING',
        'release_date': 'DATE',
        'popularity': 'FLOAT',
        'vote_average': 'FLOAT',
        'vote_count': 'INTEGER'
    },
    'data_root': ['results'],
    'row_id': ['id'],
    'arg_fields': {
        'year': {
            # 'data_type': 'INTEGER',
            'param' : 'primary_release_year'
        },
        'sort_by': {
            # 'data_type': 'STRING',
            'default': 'revenue.desc',
            'param': 'sort_by'
        },
        'page': {
            # 'data_type': 'INTEGER',
            'default': 1,
            'param': 'page'
        }
    }
}

CREDITS_RAW = {
    'api_path': 'movies_credits',
    'raw_schema': {
        'id': 'INTEGER',
        'cast': 'JSON',
        'crew': 'JSON'
    },
    'row_id':  ['id'],
    'arg_fields': {
        'movie': {
            # not including data_type because not in the final BQ table
            'path_var': 'movie_id'
        }
    }
}