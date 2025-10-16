from google.cloud import bigquery
import logging, time
from contextlib import contextmanager
from core.env import resolve_project
from core.utils import format_stage_merge_query
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

# -------------------------------------------------
# Manage BigQuery Client
# -------------------------------------------------

# Always used to create the client
def get_bq_client(project_id=None):
    '''
    Creates and returns a new BigQuery client.
    If project_id is None, defaults from environment/credentials.
    '''
    project_id = resolve_project(project_id)
    logger.debug(f'Creating BigQuery client (project_id: {project_id})')
    return bigquery.Client(project=project_id)


# Explicitly manage client lifecycle
@contextmanager
def bq_client_context(project_id=None):
    '''
    Context manager that yields a BigQuery client and ensures it is closed.
    Usage:
        with bq_client_context('my-project') as client:
            ...
    '''
    client = get_bq_client(project_id=project_id)
    try:
        yield client
    finally:
        client.close()


# Decorator wraps functions for Airflow
def with_client(func):
    '''
    Wrapper to handle opening/closing a BigQuery Client if one is not passed explicitly. 
    Internally uses the same bq_client_context().
    If project_id is not passed, this wrapper replaces it so it can be referenced
        in the wrapped function
    '''
    def wrapper(*args, client=None, project_id=None, **kwargs):
        if client is not None:
            # Client is provided, don't bother opening/closing it, but do populate project_id
            return func(*args, client=client, project_id=client.project, **kwargs)
        
        # Otherwise, open a managed client context
        with bq_client_context(project_id) as managed_client:
            return func(*args, client=managed_client, project_id=managed_client.project, **kwargs)
    return wrapper


# -------------------------------------------------
# Resource Creation
# -------------------------------------------------

@with_client
def create_dataset(resource_id, client=None, project_id=None):
    '''Creates a new dataset from dataset_id or dataset_id.table_id'''
    dataset_ref = get_dataset_ref(resource_id, client=client, project_id=project_id)
    try:
        client.create_dataset(dataset_ref)
        logger.info(f'Created dataset {project_id}.{dataset_ref.dataset_id}')
    except Exception as e:
        if 'Already Exists' in str(e):
            logger.info(f'Dataset {project_id}.{dataset_ref.dataset_id} already exists.')
        else:
            raise(e)

@with_client
def create_table(dataset_table, schema, partition_config=None, clustering=None,
                 client=None, project_id=None):
    table_ref = get_table_ref(dataset_table, client=client, project_id=project_id)
    table = bigquery.Table(table_ref, schema=schema)
    if partition_config:
        table.time_partitioning = create_time_partitioning(partition_config)
    if clustering:
        table.clustering_fields = clustering
    try:
        client.create_table(table)
        logger.info(f'Created table {project_id}.{dataset_table}')
    except Exception as e:
        if 'Already Exists' in str(e):
            logger.info(f'Table {project_id}.{dataset_table} already exists.')
        else:
            raise(e)
    return


def create_time_partitioning(partition_config):
    if not partition_config:
        return None
    return bigquery.TimePartitioning(
        type_=getattr(bigquery.TimePartitioningType, partition_config['type']),
        field=partition_config['field']
    )


# -------------------------------------------------
# Cleanup & Checks
# -------------------------------------------------
@with_client
def delete_table(dataset_table, client=None, project_id=None):

    table_ref = get_table_ref(dataset_table, client=client, project_id=project_id)
    try:
        client.delete_table(table_ref)
        logger.info(f'Deleted table {project_id}.{dataset_table}')
    except Exception as e:
        if 'Not found' not in str(e):
            raise (e)
        else:
            logger.info(f'Table {project_id}.{dataset_table} not found, could not delete')
    return

@with_client
def table_exists(dataset_table, retries=5, wait=2,
                 client=None, project_id=None):
    for _ in range(retries):
        try:
            client.get_table(dataset_table)
            return dataset_table
        except NotFound:
            time.sleep(wait)
    return False

# -------------------------------------------------
# Helpers
# -------------------------------------------------
@with_client
def get_table_ref(dataset_table, client=None, project_id=None):
    dataset_id, table_id = dataset_table.split('.')
    table_ref = client.dataset(dataset_id).table(table_id)
    return table_ref

@with_client
def get_dataset_ref(resource_id, client=None, project_id=None):
    if '.' in resource_id:
        dataset_id, _ = resource_id.split('.')
    else:
        dataset_id = resource_id
    dataset_ref = client.dataset(dataset_id)
    return dataset_ref

# -------------------------------------------------
# Data Manipulation and Loading
# -------------------------------------------------

@with_client
def bq_merge(schema, merge_cols, staging_table, final_table, client=None, project_id=None):
    query = format_stage_merge_query(
        staging_table=staging_table,
        final_table=final_table,
        schema=schema,
        merge_cols=merge_cols,
    )
    logger.debug(f'''QUERY = {query}''')
    client.query(query).result()
    return final_table


@with_client
def load_all_gcs_to_bq(gcs_uris, dataset_table, client=None, project_id=None):
    job = client.load_table_from_uri(
        gcs_uris,
        dataset_table,
        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition = 'WRITE_APPEND'
        )
    )
    job.result()
    return dataset_table







