from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import tasks.ingest as ingestion_tasks
from pipeline_utils.dag_helpers import make_gcs_path_factory
from airflow.operators.python import get_current_context

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

    @task
    def get_gcs_path(api, api_path, artist_id):
        context = get_current_context()
        make_prefix, make_file_name = make_gcs_path_factory(context)
        gcs_prefix = make_prefix(api, api_path)
        gcs_file_name = make_file_name(f'artist{artist_id}')
        return f'{gcs_prefix}/{gcs_file_name}'

    ingestion_tasks.api_fetch_and_load(
        api = API,
        api_path = api_path,
        api_args = {
            'path_vars': {'artist_id': ARTIST_ID},
            'token_data': get_token()
        },
        gcs_path = get_gcs_path(API, api_path, ARTIST_ID),
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
