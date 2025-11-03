from airflow.decorators import dag
from tasks.transform import gcs_transform_for_bq
from task_groups.load import gcs_to_bigquery
from config.datasources import SSA_STATE_NAMES

@dag()
def ssa_state_names():

    transformed_uris = gcs_transform_for_bq(config=SSA_STATE_NAMES)

    gcs_to_bigquery(transformed_uris, config=SSA_STATE_NAMES)
    
ssa_state_names()