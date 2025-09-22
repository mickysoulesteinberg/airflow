import os, requests, logging
from tenacity import retry, wait_exponential, stop_after_attempt

# Configure logger
logger = logging.getLogger(__name__)

# TMDb API config
API_KEY = os.getenv("TMDB_API_KEY")  # store key in .env
BASE_URL = 'https://api.themoviedb.org/3'

if not API_KEY:
    raise EnvironmentError('TMDB_API_KEY not found in environment variables.')

# Generic TMDb API call with retries
@retry(wait = wait_exponential(multiplier = 1, min = 2, max = 10), stop = stop_after_attempt(5))
def tmdb_get(endpoint: str, **params) -> dict:
    '''
    Generic TMDb API call # with retries.
    Automatically adds API key to params.

    Args:
        endpoing: e.g. 'discover/movie' or f'movie/{id}/credits'
        params: query parameters as key-value pairs
    
    Returns:
        dict: JSON response parsed to dictionary
    '''
    url = f'{BASE_URL}/{endpoint.lstrip('/')}'
    query_params = {'api_key': API_KEY, **params}
    logger.debug(f'Requesting TMDb endpoint: {url} with params: {params}')

    response = requests.get(url, params = query_params)
    response.raise_for_status()
    return response.json()

# ---- Wrappers for common endpoints ----
def discover_movies_by_year(
        year: int,
        sort_by: str = 'revenue',
        sort_order: str = 'desc',
        page: int = 1
) -> dict:
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

    # Validate sort_by and sort_order
    valid_sort_fields = {
        'popularity', 'release_date', 'revenue', 'primary_release_date',
        'original_title', 'vote_average', 'vote_count'
    }
    if sort_by not in valid_sort_fields:
        raise ValueError(f"Invalid sort_by field: {sort_by}. Must be one of {valid_sort_fields}.")
    if sort_order not in {'asc', 'desc'}:
        raise ValueError("sort_order must be 'asc' or 'desc'.")
    
    sort_param = f'{sort_by}.{sort_order}'
    return tmdb_get(
        'discover/movie',
        primary_release_year = year,
        sort_by = sort_param,
        page = page
    )

def get_credits(movie_id: int) -> dict:
    '''
    Wrapper for movie/{movie_id}/credits endpoint.
    
    Args:
        movie_id (int): TMDb movie ID
        
    Returns:
        dict: JSON response parsed to dictionary
    '''
    return tmdb_get(f'movie/{movie_id}/credits')

def get_movie_details(movie_id: int) -> dict:
    '''
    Wrapper for movie/{movie_id} endpoint.
    
    Args:
        movie_id (int): TMDb movie ID   
    '''
    return tmdb_get(f'movie/{movie_id}')