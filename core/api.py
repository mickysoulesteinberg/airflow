import os, requests, logging, time
from utils.config import CONFIG

# Get the configuration settings for APIs
API_CONFIG = CONFIG['apis']

# Configure logger
logger = logging.getLogger(__name__)

# Get token for OAuth
def get_oauth2_token(api):
    '''Retrieve an OAuth token and expiration'''
    # Client Credentials from .env
    client_id = os.getenv(f'{api.upper()}_CLIENT_ID')
    client_secret = os.getenv(f'{api.upper()}_CLIENT_SECRET')
    if not client_id or not client_secret:
        raise ValueError(f'{api.upper()}_CLIENT_ID and {api.upper()}_CLIENT_SECRET must be set in environment variables')
    
    # API Settings from settings.yaml
    api_config = API_CONFIG[api]
    token_url = api_config['token_url']
    if not token_url:
        raise ValueError(f'token_url not set for {api} in settings.yaml')

    # Get token and compute expiry
    r = requests.post(
        token_url,
        data = {'grant_type': 'client_credentials'},
        auth = (client_id, client_secret)
    )
    r.raise_for_status()
    data = r.json()
    access_token = data['access_token']
    expires_in = data['expires_in']
    expires_at = int(time.time()) + expires_in
    return {'access_token': access_token, 'expires_at': expires_at}


# --- Helpers for API GET call --- #
def api_oauth2_auth(api, token_data = None, refresh_token = True):
    '''Helper GET Request for OAuth2''' 
    if token_data is None and refresh_token is False:
        raise ValueError('No access token provided.')
    if int(time.time()) >= token_data['expires_at']:
        if refresh_token is False:
            raise ValueError('The provided token has expired')
        else:
            token_data = get_oauth2_token(api)
    access_token = token_data['access_token']
    return {'Authorization': f'Bearer {access_token}'}
    
def api_key_auth(api, params):
    api_key = os.getenv(f'{api.upper()}_API_KEY')
    if not api_key:
        raise ValueError(f'{api.upper()}_API_KEY must be set in environment variables')
    return {**( params or {}), 'api_key': api_key}

def build_url(base_url, endpoint):
    return f'{base_url.rstrip('/')}/{endpoint.lstrip('/')}'

# --- Generic API GET call --- #
def api_get(api, path, path_vars = None, params = None, token_data = None, refresh_token = False):
    '''
    Generic GET request helper

    Args:
        api (str): Name of the API, e.g. 'tmdb', 'spotify'
        endpoint (str): path to the endpoint 
        params (dict, optional): Query string parameters for the request
        creds (dict, optional): Token information dictionar
    
    Returns:
        json from request
    '''
    api_config = API_CONFIG[api]
    base_url = api_config['base_url']

    path_config = api_config['paths'][path]

    # Build endpoint URL
    endpoint = path_config['endpoint']
    if path_vars:
        endpoint = endpoint.format(**path_vars)

    url = build_url(base_url, endpoint)

    # Headers: merge global default and endpoint-specific
    headers = api_config.get('default_headers', {}).copy()
    headers.update(path_config.get('headers', {}))

    # Authentication
    if api_config['auth'] == 'oauth2':
        auth_headers = api_oauth2_auth(api, token_data = token_data, refresh_token = refresh_token)
        headers.update(auth_headers)
    if api_config['auth'] == 'api_key':
        params = api_key_auth(api, params)

    # For debugging
    log_string = f'''
    ------------------------------------------------
    Submitting API with params:
    url = {url}
    headers = {headers}
    params = {params}
    '''
    logger.debug(log_string)

    # Call API
    r = requests.get(url, headers = headers, params = params)
    r.raise_for_status()

    logger.debug(f'API GET {url} -> {r.status_code}')
    
    # 204 No content
    if r.status_code == 204:
        logger.warning(f'No content returned for {url}')
        return None

    # Other non-200
    if r.status_code != 200:
        raise RuntimeError(
            f'{api} error {r.status_code} for url {url}: {r.text[:500]}'
        )

    try:
        return r.json()
    except ValueError:
        raise ValueError(f'Response not JSON: {r.text[:500]}')