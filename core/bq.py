from google.cloud import bigquery
import logging, os, textwrap, json
from datetime import datetime, UTC

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

logger = logging.getLogger(__name__)

# Get a BQ Client
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


def format_stage_merge_query(staging_table, final_table, schema, merge_cols):
    schema_cols = [col['name'] for col in schema]
    on_clause = ' AND '.join([f'F.{col} = S.{col}' for col in merge_cols])
    update_clause = ',\n    '.join([
        'last_updated = CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'{col} = S.{col}'
        for col in schema_cols
        if col not in merge_cols
    ])
    insert_cols = ', '.join(schema_cols)
    values_clause = ', '.join([
        'CURRENT_TIMESTAMP()' if col == 'last_updated'
        else f'S.{col}'
        for col in schema_cols
    ])

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

def bq_current_timestamp():
    return datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')


def transform_record(record, schema_config, context_values):
    row = {}
    for col in schema_config:
        name = col['name']
        json_path = col.get('json_path')
        col_type = col.get('type')

        if json_path:
            value = record.get(json_path)
        elif name in context_values:
            value = context_values[name]
        else:
            value = None

        if col_type == 'JSON' and value is not None:
            value = json.dumps(value)

        row[name] = value
    return row