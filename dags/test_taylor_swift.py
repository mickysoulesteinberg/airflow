from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import utils.spotify as spotify

@dag(
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['spotify']
)
def taylor_swift_dag():
    '''
    Simple Test DAG
    - Get Spotify Token
    - Fetch Taylor Swift's Artist Data
    - Fetch Taylor Swift's Top Tracks
    '''

    @task
    def get_creds():
        return spotify.get_spotify_token()
    
    @task
    def fetch_artist(creds):
        artist_id = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID
        artist_data = spotify.spotify_get(f'artists/{artist_id}', creds=creds)
        return artist_data

    @task
    def fetch_top_tracks(creds):
        artist_id = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID
        top_tracks_data = spotify.spotify_get(f'artists/{artist_id}/top-tracks', params={'market': 'US'}, creds=creds)
        return top_tracks_data  
    
    creds = get_creds()
    fetch_artist(creds)
    fetch_top_tracks(creds)

taylor_swift_dag()