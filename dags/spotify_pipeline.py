from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from task_groups.ingestion import api_fetch_and_load
from core.api import get_oauth2_token

API = 'spotify'
DAG_ID = 'spotify_pipeline'
ARTIST_ID = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID

@dag(start_date=days_ago(1), schedule="@daily", catchup=False)
def spotify_pipeline():
    # dag_status = StatusTable() #todo: set this up so can track status of dag at each step
    
    @task
    def get_token():
        token_data = get_oauth2_token(API)
        return token_data
    
    token_data = get_token()

    path_vars = {
        'artist_id': ARTIST_ID
    }

    # Run Task Group to load movies to GCS & BigQuery, retain list of movie IDs for second run
    api_fetch_and_load(
        api = API,
        dag_id = DAG_ID,
        api_path = 'artists',
        api_path_vars = path_vars,
        # params = optional
        api_token_data = token_data
    )

spotify_pipeline()

# artist_data = _scratch_spotify.spotify_get(f'artists/{artist_id}', creds=creds)
# api_get('spotify', 'artists/{artist_id}', path_vars = {'artist_id': ARTIST_ID})
