TMDB_CONFIG_RAW = {
    'datasource_name': 'tmdb',
    'description': 'Data from API TMDB',
    'default_config': {
        'api_name': 'tmdb'
    }
}

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
    'api_arg_fields': {
        'year': {
            'param' : 'primary_release_year'
        },
        'sort_by': {
            'default': 'revenue.desc',
            'param': 'sort_by'
        },
        'page': {
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
    'api_arg_fields': {
        'movie': {
            # not including data_type because not in the final BQ table
            'path_var': 'movie_id'
        }
    }
}