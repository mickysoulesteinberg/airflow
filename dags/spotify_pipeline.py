from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import tasks.ingestion as ingestion_tasks

from core.api import get_oauth2_token
import logging

logger = logging.getLogger(__name__)

ARTIST_ID = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID
API = 'spotify'

@dag(
        start_date=days_ago(1), 
        schedule="@daily", 
        catchup=False
)
def spotify_pipeline():    
    @task
    def get_token():
        token_data = get_oauth2_token(API)
        return token_data

    api_path = 'artists'

    # Get the GCS Path to store the data
    # TODO replace with get_gcs_path_for_task and delete get_storage_data task
    gcs_path = ingestion_tasks.get_storage_data(
        api = API,
        api_path = api_path,
        call_params = {'artist': ARTIST_ID}
    )

    ingestion_tasks.api_fetch(
        api = API,
        api_path = api_path,
        api_args = {
            'path_vars': {'artist_id': ARTIST_ID},
            'token_data': get_token()
        },
        gcs_path = gcs_path,
        return_data = {
            'artist_id': 'id',
            'artist_name': 'name',
            'artist_popularity': 'popularity',
            'spotify_url': 'external_urls.spotify',
            'genres': 'genres[]', #empty for taylor
            'main_image': 'images[0].url',
            'thumbnail_image': 'images[-1].url',
            'all_image_urls': 'images[].url',
            'all_image_heights': 'images[].height',
            'followers': 'followers.total'
        }
    )


spotify_pipeline()
