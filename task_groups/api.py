from airflow.decorators import task_group
import tasks.ingest as ingestion_tasks
import tasks.transform as transform_tasks

from config.logger import get_logger
logger = get_logger(__name__)


@task_group
def fetch_and_prep(gcs_prefix, *, config=None, return_data=None, **overrides):
    
    call_builder = ingestion_tasks.setup_api_call(config=config, **overrides)

    api_args = call_builder['api_args']
    gcs_file_name = call_builder['gcs_file_name']
    api_call_kwargs = call_builder['api_call_kwargs']

    fetched = ingestion_tasks.api_fetch_and_load(
        gcs_file_name, gcs_prefix, config=config, return_data=return_data, metadata=api_call_kwargs, **overrides
    )

    result = {key: fetched[key] for key in return_data or {}}

    loaded_gcs_path = fetched['gcs_path']

    transformed_uri = transform_tasks.gcs_transform_for_bq(config=config, gcs_path=loaded_gcs_path)

    result['transformed_uri'] = transformed_uri

    return result




@task_group
def fetch_and_prep_og(api, api_path, 
                        api_arg_builder, arg_fields,
                        gcs_prefix, table_config, data_config,
                        return_data=None,
                        **kwargs):
    
    call_builder = ingestion_tasks.setup_api_call_og(
        api_arg_builder=api_arg_builder,
        arg_fields=arg_fields,
        **kwargs
    )

    api_args = call_builder['api_args']
    gcs_file_name = call_builder['gcs_file_name']
    call_metadata = call_builder['metadata']

    fetched = ingestion_tasks.api_fetch_and_load_og(api=api, api_path=api_path,
                                                    api_args=api_args,
                                                    gcs_prefix=gcs_prefix,
                                                    gcs_file_name=gcs_file_name,
                                                    return_data=return_data,
                                                    metadata=call_metadata)

    result = {key: fetched[key] for key in return_data or {}}

    loaded_gcs_path = fetched['gcs_path']

    transformed_uri = transform_tasks.gcs_transform_for_bq_og(gcs_path=loaded_gcs_path,
                                                            table_config=table_config,
                                                            data_config=data_config)
    result['transformed_uri'] = transformed_uri
    fetched >> transformed_uri

    return result

