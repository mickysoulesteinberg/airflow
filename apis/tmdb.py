import os, requests, logging
from utils.config import CONFIG
from core.api import api_get

# Configure logger
logger = logging.getLogger(__name__)

# TMDb API config
API_KEY = os.getenv("TMDB_API_KEY")  # store key in .env
API_CONFIG = CONFIG['apis']['tmdb']
BASE_URL = API_CONFIG['base_url']

if not API_KEY:
    raise EnvironmentError('TMDB_API_KEY not found in environment variables.')

# Generic TMDb API call
def tmdb_get(api_path, api_path_vars = None, api_params = None):
    return api_get('tmdb', api_path, api_path_vars = api_path_vars, api_params = api_params)


# ---- Wrappers for common endpoints ----
def discover_movies_by_year(year, sort_by = 'revenue', sort_order = 'desc', page = 1):
    '''
    Wrapper for /discover/movie endpoint with a specified year of release.

    Args:
        year (int): Year of release
        sort_by (str, optional): Field to sort by. Defaults to 'revenue'.
        sort_order (str, optional): 'asc' or 'desc'. Defaults to 'desc'.
        page (int, optional): Page number for pagination. Defaults to 1.
    
    Returns:
        dict: JSON response parsed to dictionary
    '''
    return tmdb_get('discover_movies', api_params = {'primary_release_year': year, 'sort_by': f'{sort_by}.{sort_order}', 'page': page})


def get_movie_details(movie_id):
    '''
    Wrapper for movie/{movie_id} endpoint.
    
    Args:
        movie_id (int): TMDb movie ID   
    '''
    return tmdb_get('movies', api_path_vars={'movie_id': movie_id})

def get_movie_credits(movie_id):
    '''
    Wrapper for movie/{movie_id}/credits endpoint.
    
    Args:
        movie_id (int): TMDb movie ID
        
    Returns:
        dict: JSON response parsed to dictionary
    '''
    return tmdb_get('movies_credits', api_path_vars = {'movie_id': movie_id})


