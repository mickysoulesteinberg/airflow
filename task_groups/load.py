from airflow.decorators import task_group
import tasks.load as loader_tasks
import tasks.transform as transform_tasks
import tasks.cleanup as cleanup_tasks


@task_group
def gcs_to_bigquery(transformed_uris, *, config=None, **overrides):

    created_staging_table = loader_tasks.create_staging_table(config=config, **overrides)

    loaded_staging_table = loader_tasks.gcs_to_bq_stg(
        transformed_uris,
        created_staging_table
    )

    merged_final_table = loader_tasks.bq_stg_to_final_merge(loaded_staging_table, config=config, **overrides)

    cleanup_tasks.delete_bq_staging_table(
        loaded_staging_table,
        wait_for=merged_final_table
    )

    return merged_final_table

