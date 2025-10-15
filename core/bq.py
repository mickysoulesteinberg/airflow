from google.cloud import bigquery
import logging, os, textwrap, time
from google.api_core.exceptions import NotFound
from contextlib import contextmanager
from core.env import resolve_project

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
def create_dataset_if_not_exists(dataset_id, client=None, project_id=None):
    dataset_ref = bigquery.Dataset(f'{project_id}.{dataset_id}')
    try:
        client.create_dataset(dataset_ref)
        logger.info(f'Created dataset {dataset_id}.')
    except Exception as e:
        if 'Already Exists' in str(e):
            logger.info(f'Dataset {dataset_id} already exists.')
        else:
            raise


@with_client
def create_table(dataset_table,
                 table_config=None, schema_config=None, # Must pass one of these
                 client=None, project_id=None,
                 force_recreate=False, confirm_creation=False,
                 retries=5, wait=2):
    
    schema = schema_config or table_config['schema']
    if not schema:
        logger.warning('No schema provided. Please input table_config or schema_config')
        return

    dataset_id, table_id = dataset_table.split('.')

    # Ensure dataset exists
    create_dataset_if_not_exists(dataset_id, client=client) #Client has been created with wrapper

    table_ref = client.dataset(dataset_id).table(table_id)

    # Drop table if force_recreate
    if force_recreate:
        try:
            client.delete_table(table_ref)
            logger.warning(f'Dropped table {dataset_table} (force_recreate=True)')
        except Exception as e:
            if 'Not found' not in str(e):
                raise #TODO confirm raising error does not end the call
            
    table = bigquery.Table(table_ref, schema=schema)

    if table_config:
    # Partitioning
        if 'partition' in table_config:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=getattr(bigquery.TimePartitioningType, table_config['partition']['type']),
                field=table_config['partition']['field'],
            )

        # Clustering
        if 'clustering' in table_config:
            table.clustering_fields = table_config['clustering']

    try:
        client.create_table(table)
        logger.info(f'Created table {dataset_table}')
    except Exception as e:
        if 'Already Exists' in str(e):
            logger.info(f'Table {dataset_table} exists')
        else:
            raise

    # If we want to confirm successful creation before returning
    if confirm_creation:
        for _ in range(retries):
            try:
                client.get_table(dataset_table)
                return dataset_table
            except NotFound:
                time.sleep(wait)

        raise RuntimeError(f"Table {dataset_table} not found after {retries} retries")
    return dataset_table


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
    return


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
    return

# -------------------------------------------------
# Formatting
# -------------------------------------------------

def format_stage_merge_query(staging_table, final_table, schema, merge_cols):
    schema_cols = [col['name'] for col in schema]

    # Define clauses
    on_clause = ' AND '.join([f'F.`{col}` = S.`{col}`' for col in merge_cols])
    update_clause = ',\n    '.join([
        'F.`last_updated` = CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'F.`{col}` = S.`{col}`'
        for col in schema_cols
        if col not in merge_cols
    ])
    insert_cols = ', '.join([f'`{col}`' for col in schema_cols])
    values_clause = ', '.join([
        'CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'S.`{col}`'
        for col in schema_cols
    ])

    # Construct query
    query = f'''
    MERGE `{final_table}` F
    USING `{staging_table}` S
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols})
    VALUES ({values_clause});
    '''

    return textwrap.dedent(query).strip()








