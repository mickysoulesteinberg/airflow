from airflow.decorators import dag, task_group
import tasks.ingest as ingestion_tasks
from config.datasources import SPOTIFY_ARTISTS

from config.logger import get_logger
logger = get_logger(__name__)

ARTIST_ID = '06HL4z0CvFAxyc27GXpf02'  # Taylor Swift's Spotify Artist ID

@dag()
def artists():

    config = SPOTIFY_ARTISTS
    initial_setup = ingestion_tasks.setup_etl(**config, get_token=True)

    call_builders = ingestion_tasks.setup_api_call(
        
    )