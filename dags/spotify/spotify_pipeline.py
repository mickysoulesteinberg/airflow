from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import tasks.ingest as ingestion_tasks
from pipeline.dag_helpers import make_gcs_path_factory
from airflow.operators.python import get_current_context

from core.api import get_oauth2_token
from config.logger import get_logger

logger = get_logger(__name__)

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

    @task(multiple_outputs=True)
    def get_gcs_path(api, api_path, artist_id):
        context = get_current_context()
        make_prefix, make_file_name = make_gcs_path_factory(context)
        gcs_prefix = make_prefix(api, api_path)
        gcs_file_name = make_file_name(f'artist{artist_id}')
        return {'gcs_prefix': gcs_prefix, 'gcs_file_name': gcs_file_name}
        # return join_gcs_path(gcs_prefix, gcs_file_name)

    gcs_path = get_gcs_path(API, api_path, ARTIST_ID)

    ingestion_tasks.api_fetch_and_load(
        gcs_path['gcs_file_name'],
        gcs_path['gcs_prefix'],
        api_name=API,
        api_path=api_path,
        api_args = {
            'path_vars': {'artist_id': ARTIST_ID},
            'token_data': get_token()
        },
        return_data=return_data
    )




spotify_pipeline()
