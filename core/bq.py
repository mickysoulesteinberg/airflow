from google.cloud import bigquery
import logging, os, textwrap, time
from google.api_core.exceptions import NotFound
from core.gcs import load_json_from_gcs, bq_current_timestamp, transform_record
# from core.transform import transform_record

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


def insert_row_json(dataset, table, row, project_id=None):
    '''
    Insert a single row (dict) into BigQuery table.
    '''
    client = get_bq_client(project_id)
    table = f'{client.project}.{dataset}.{table}'
    errors = client.insert_rows_json(table, [row])

    if errors:
        logger.error(f'BigQuery insert failed: {errors}')
        raise RuntimeError(f'BigQuery insert failed: {errors}')
    logger.info(f'Inserted row into {table}')
    return row

def insert_rows_json(dataset_table, rows, project_id = None):
    client = get_bq_client(project_id)
    errors = client.insert_rows_json(dataset_table, rows)
    client.close()
    if errors:
        raise RuntimeError(errors)
    logger.info(f'Inserted {len(rows)} rows into {dataset_table}')


# def make_bq_schema(schema_fields):
#     '''
#     Convert list of dicts into list of BigQuery SchemaField objects.
#     '''
#     return [
#         bigquery.SchemaField(
#             name=f['name'],
#             field_type=f['type'],
#             mode=f.get('mode', 'NULLABLE'),
#         )
#         for f in schema_fields
#     ]

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



# def create_table(dataset_table, schema, project_id, retries=5, wait=1):
#     client = bigquery.Client(project=project_id)

#     # Wait until it’s really available
#     for _ in range(retries):
#         try:
#             client.get_table(table_id)
#             return table_id   # return the string so downstream can use it
#         except NotFound:
#             time.sleep(wait)

#     raise RuntimeError(f"Table {table_id} not found after {retries} retries")


def create_table(
        dataset_table,
        table_config = None,
        schema_config = None,
        client = None,
        project_id = None,
        force_recreate = False,
        retries = None,
        wait = 2
):
    schema = schema_config or table_config['schema']
    if not schema:
        logger.warning('No schema provided. Please input table_config or schema_config')
        return

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
    if retries:
        for _ in range(retries):
            try:
                client.get_table(dataset_table)
                return dataset_table
            except NotFound:
                time.sleep(wait)

        raise RuntimeError(f"Table {dataset_table} not found after {retries} retries")
    return dataset_table

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








def transform_and_insert_json(schema_config, dataset_table, gcs_path, project_id = None, json_root = None):
    json_data = load_json_from_gcs(gcs_path, project_id)

    # Define context values to use for static/generated columns
    context_values = {
        'gcs_uri': gcs_path,
        'last_updated': bq_current_timestamp()
    }

    # Drill down if json_root is provided
    if json_root:
        for key in json_root:
            if isinstance(json_data, dict) and key in json_data:
                json_data = json_data[key]
            else:
                raise KeyError(f"json_root step '{key}' not found in JSON at {gcs_path}")

    # At this point, json_data could be a dict (single record) or list (multiple records)
    if isinstance(json_data, list):
        rows = [transform_record(record, schema_config, context_values) for record in json_data]
    else:
        rows = [transform_record(json_data, schema_config, context_values)]
    
    # load to BQ
    client = get_bq_client(project_id)
    job = client.load_table_from_json(rows, dataset_table)
    client.close()

    return job.result()

def bq_merge(schema, merge_cols, staging_table, final_table):
    client = get_bq_client()
    query = format_stage_merge_query(
        staging_table=staging_table,
        final_table=final_table,
        schema=schema,
        merge_cols=merge_cols,
    )
    logger.warning(f'''QUERY = {query}''')
    client.query(query).result()
    client.close()
    return

def load_all_gcs_to_bq(gcs_uris, dataset_table):
    client = get_bq_client()
    job = client.load_table_from_uri(
        gcs_uris,
        dataset_table,
        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition = 'WRITE_APPEND'
        )
    )
    job.result()
    client.close()
    return


