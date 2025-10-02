from google.cloud import bigquery
import logging, os

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

logger = logging.getLogger(__name__)

def get_bq_client(project_id=PROJECT_ID):
    '''
    Creates and returns a new BigQuery client.
    If project_id is None, defaults from environment/credentials.
    '''
    logger.debug(f'Creating BigQuery client (project_id={project_id})')
    return bigquery.Client(project=project_id) if project_id else bigquery.Client()

def insert_row_json(table_id, row, project_id=None):
    '''
    Insert a single row (dict) into BigQuery table.
    '''
    client = get_bq_client(project_id)
    errors = client.insert_rows_json(table_id, [row])

    if errors:
        logger.error(f'BigQuery insert failed: {errors}')
        raise RuntimeError(f'BigQuery insert failed: {errors}')
    logger.info(f'Inserted row into {table_id}')
    return row

def make_bq_schema(schema_fields):
    '''
    Convert list of dicts into list of BigQuery SchemaField objects.
    '''
    return [
        bigquery.SchemaField(
            name=f['name'],
            field_type=f['type'],
            mode=f.get('mode', 'NULLABLE'),
        )
        for f in schema_fields
    ]

def create_dataset_if_not_exists(dataset_id, client = None, project_id = None):
    project_id = project_id or PROJECT_ID
    client = client or get_bq_client(project_id)
    dataset_ref = bigquery.Dataset(f'{project_id}.{dataset_id}')
    try:
        client.create_dataset(dataset_ref)
        logger.info(f'Created dataset {dataset_id}')
    except Exception as e:
        if 'Already Exists' in str(e):
            logger.info(f'Dataset {dataset_id} exists')
        else:
            raise

def create_table_if_not_exists(
        dataset_table,
        table_config,
        client = None,
        project_id = None,
        force_recreate = False
):
    project_id = project_id or PROJECT_ID
    client = client or get_bq_client(project_id)
    dataset_id, table_id = dataset_table.split('.')

    # Ensure dataset exists
    create_dataset_if_not_exists(dataset_id, client = client, project_id = project_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    # Drop table if force_recreate
    if force_recreate:
        try:
            client.delete_table(table_ref)
            logger.warning(f'Dropped table {dataset_table} (force_recreate=True)')
        except Exception as e:
            if 'Not found' not in str(e):
                raise
            
    schema = make_bq_schema(table_config['schema'])
    table = bigquery.Table(table_ref, schema=schema)

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



# # probably delete or replace
# def insert_rows(api: str, table: str, rows: list[dict]):
#     '''
#     Generic BigQuery insert
    
#     Args:
#         api: which API config to use (e.g. 'tmdb', 'spotify')
#         table: table name
#         rows: list of dicts to insert
#     '''
#     dataset = CFG[api]['bq_dataset_staging']
#     table_id = f'{project_id}.{dataset}.{table}'

#     if not rows:
#         logger.info(f'[BQ] No rows to insert into {table_id}')
#         return  

#     errors = client.insert_rows_json(table_id, rows)
#     if errors:
#         logger.error(f'[BQ] Insert errors for {table_id}: {errors}')
#         raise RuntimeError(errors)
    
#     logger.info(f'[BQ] Inserted {len(rows)} rows into {table_id}')
#     return

# def load_to_bq(data, table, dag_id = None, group_id = None):
#     print('Uploading to BigQuery (This function is in progress)')
