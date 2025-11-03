# from airflow.decorators import dag
# from task_groups.load import gcs_to_bigquery
# from tasks.transform import gcs_transform_for_bq
# from config.datasources_og import SSA_TERRITORY_NAMES

# from config.logger import get_logger
# logger = get_logger(__name__)

# @dag()
# def ssa_territory_names():

#     transformed_uris = gcs_transform_for_bq(SSA_TERRITORY_NAMES)

#     gcs_to_bigquery(SSA_TERRITORY_NAMES, transformed_uris)

# ssa_territory_names()