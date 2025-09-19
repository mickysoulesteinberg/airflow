import os
import base64
import requests
import time



def get_spotify_token() -> dict:
    """
    Request access token from Spotify using client credentials flow.
    
    Environment Variables:
        SPOTIFY_CLIENT_ID: Your Spotify Client ID
        SPOTIFY_CLIENT_SECRET: Your Spotify Client Secret

    Returns:
        dict: A dictionary with keys:
            - access_token (str): The access token string
            - token_type (str): The type of the token, typically 'Bearer'
            - expires_at (int): Unix timestamp when the token expires
    """
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError("Spotify client ID and secret must be set in environment variables.")
    
    def get_auth_header(client_id, client_secret):
        # Helper function to create the Authorization header using client credentials
        creds = f'{client_id}:{client_secret}'
        creds_b64 = base64.b64encode(creds.encode()).decode()
        return f'Basic {creds_b64}'

    def make_spotify_headers(token):
        # Helper function to create headers for Spotify Web API requests using the access token
        authorization = f'Bearer {token}'
        content_type = 'application/json'
        return {
            'Authorization': authorization,
            'Content-Type': content_type
        }
    
    auth_header = get_auth_header(client_id, client_secret)
    
    r = requests.post(
        'https://accounts.spotify.com/api/token',
        headers={
            'Authorization': auth_header,
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        data={'grant_type': 'client_credentials'}
    )
    r.raise_for_status()

    data = r.json()
    access_token = data['access_token']
    token_type = data['token_type']
    expires_in = data['expires_in']
    expires_at = int(time.time()) + expires_in

    return {
        'access_token': access_token,
        'token_type': token_type,
        'expires_at': expires_at,
        'headers': make_spotify_headers(access_token)
    }

def spotify_get(
        endpoint: str,
        params: dict = None,
        creds: dict = None
):
    """
     Generic GET request helper for Spotify Web API.

     Args:
        endpoint (str): Path after 'https://api.spotify.com/v1/' 
        params (dict, optional): Query string parameters for the request.
        creds (dict, optional): Token information dictionary as returned by get_spotify_token().
    
    Returns:
        dict: The JSON response from the Spotify API.
    """

    # Validate creds
    if creds is None:
        raise ValueError("creds must be provided.")
    token_expiry = creds.get('expires_at', 0)
    if int(time.time()) >= token_expiry:
        raise ValueError("The provided token has expired.")
    
    base_url = 'https://api.spotify.com/v1/'
    headers = creds['headers']
    # access_token = creds['access_token']
    # headers = {'Authorization': f'Bearer {access_token}'}    
    
    url = base_url + endpoint.lstrip('/')
    
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    
    return r.json()
