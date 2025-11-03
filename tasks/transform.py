from airflow.decorators import task
import pipeline.transform as pipeline
from pipeline.transform import gcs_transform_and_store
from config.datasources import BQ_METADATA_COL, RAW_DATA_KEY
from config.logger import get_logger
from core.utils import collect_list

logger = get_logger(__name__)


@task
def gcs_transform_for_bq(config=None, **overrides):
    return pipeline.gcs_transform_for_bq(config=config, **overrides)

