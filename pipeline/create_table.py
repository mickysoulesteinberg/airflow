from core.bq import create_dataset, delete_table, table_exists, with_client, create_table
from core.logger import get_logger

logger = get_logger(__name__)

@with_client
def create_table_from_config(dataset_table,
                table_config=None, schema_config=None,
                partition=None, clustering=None,
                client=None, project_id=None,
                force_recreate=False, confirm_creation=False):

    if table_config:
        schema_config = schema_config or table_config.get('schema')
        partition = partition or table_config.get('partition')
        clustering = clustering or table_config.get('clustering')

    if not schema_config:
        logger.warning('No schema provided. Please input table_config or schema_config')
        return

    # Ensure dataset exists
    create_dataset(dataset_table, client=client, project_id=project_id) 

    # Drop table if force_recreate
    if force_recreate:
        delete_table(dataset_table, client=client, project_id=project_id)
    
    create_table(dataset_table, schema_config,
                 partition_config=partition, clustering=clustering,
                 client=client, project_id=project_id)

    # If we want to confirm successful creation before returning
    if confirm_creation:
        dataset_table = table_exists(dataset_table, client=client, project_id=project_id)
        if dataset_table is None:
            raise RuntimeError(f'Table {dataset_table} not found after creation')
        return dataset_table

    return dataset_table