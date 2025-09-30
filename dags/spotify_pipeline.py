from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from task_groups.ingestion import api_to_gcs
from core.api import get_oauth2_token
import logging

logger = logging.getLogger(__name__)

API = 'spotify'
DAG_ID = 'spotify_pipeline'
ARTIST_ID = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID
# TOKEN_DATA = get_oauth2_token(API)
# logger.warning(f'TOKEN_DATA={TOKEN_DATA}')

@dag(
        start_date=days_ago(1), 
        schedule="@daily", 
        catchup=False
    )
def spotify_pipeline():    
    @task
    def get_token(**context):
        token_data = get_oauth2_token(API)
        return token_data

    api_path_vars = {
        'artist_id': ARTIST_ID
    }

    # Run Task Group to load movies to GCS & BigQuery, retain list of movie IDs for second run
    api_to_gcs(
        api = API,
        api_path = 'artists',
        api_args = {
            'path_vars': api_path_vars,
            'token_data': get_token(),
            'call_id': f'artist{ARTIST_ID}'
        }

    )

spotify_pipeline()

# artist_data = _scratch_spotify.spotify_get(f'artists/{artist_id}', creds=creds)
# api_get('spotify', 'artists/{artist_id}', api_path_vars = {'artist_id': ARTIST_ID})
