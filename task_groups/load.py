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




@task_group
def gcs_to_bigquery_og(config=None,
                    table_config=None, schema_config=None,
                    bigquery_config=None, bigquery_table_name=None,
                    transformed_uris=None):
    
    if not transformed_uris:
        raise ValueError('transformed_uris must be provided')
    
    

    table_config = config['table_config']
    bigquery_config = config['bigquery_config']
    # data_config = config['data_config']
    # storage_config = config['storage_config']

    bq_schema_config = table_config['schema']
    bq_table_name = bigquery_table_name or bigquery_config.get('table')
    bq_dataset = bigquery_config['dataset']

    created_staging_table = loader_tasks.create_staging_table_og(
        schema_config=bq_schema_config,
        final_table=bq_table_name,
        dataset=bq_dataset
    )

    loaded_staging_table = loader_tasks.gcs_to_bq_stg(
        transformed_uris,
        created_staging_table
    )

    merged_final_table = loader_tasks.bq_stg_to_final_merge_og(
        schema_config=bq_schema_config,
        staging_table=loaded_staging_table,
        final_table=bq_table_name,
        dataset=bq_dataset,
        table_config=table_config
    )

    cleanup_tasks.delete_bq_staging_table(
        loaded_staging_table,
        wait_for=merged_final_table
    )

