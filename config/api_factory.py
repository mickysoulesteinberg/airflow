from config.logger import get_logger
logger = get_logger(__name__)

BQ_METADATA_COL = 'airflow_metadata'
BQ_TIMESTAMP_COL = 'last_updated'

def create_arg_builder(arg_fields, api=None, api_path=None):
    """Returns a function that builds API request args dynamically."""
    logger.trace(f'Creating arg builder for {api}.{api_path}')

    def arg_builder(**kwargs):
        logger.debug(f'arg_builder: Building args for {api}.{api_path} with kwargs={kwargs}')
        params, path_vars, call_id = {}, {}, ''

        for field, data in arg_fields.items():
            value = None
            if field in kwargs:
                value = kwargs[field]
                call_id += f'{field}{value}'
            elif 'default' in data:
                value = data['default']
            else:
                continue

            param = data.get('param')
            if param:
                params[param] = value

            path_var = data.get('path_var')
            if path_var:
                path_vars[path_var] = value

        built_args = {}
        if params:
            built_args['params'] = params
        if path_vars:
            built_args['path_vars'] = path_vars
        if call_id:
            built_args['call_id'] = call_id

        logger.trace(f'arg_builder: Built args for {api_path}: {built_args}')
        return built_args

    return arg_builder


def create_table_config(raw_schema, row_id=None, partition=None, context_fields=None):
    """Creates BigQuery table config with schema + metadata."""
    logger.trace('Creating table config')
    if not raw_schema:
        raise ValueError('raw_schema must be provided')

    row_id = row_id or []

    # Cols from the raw data
    schema = []
    for name, dtype in raw_schema.items():
        col = {'name': name, 'type': dtype}
        if name in row_id:
            col['mode'] = 'REQUIRED'
        schema.append(col)

    # Add context fields to row_id and schema
    context_fields = context_fields or {}
    for field, attributes in context_fields.items():
        dtype = attributes.get('type', 'STRING')
        col = {'name': field, 'type': dtype, 'source': 'CONTEXT'}
        if attributes.get('row_id'):
            col['mode'] = 'REQUIRED'
            row_id.append(field)
        schema.append(col)

    # Cols for all workflows
    schema.append({'name': BQ_METADATA_COL, 'type': 'JSON'})
    schema.append({'name': BQ_TIMESTAMP_COL, 'type': 'TIMESTAMP'})

    table_config = {'schema': schema}
    # if source_type:
    #     table_config['source_type'] = source_type
    if row_id:
        table_config['row_id'] = row_id
    if partition:
        table_config['partition'] = partition

    return table_config


def create_config(datasource_name, raw_config, dataset=None):
    """
    Create a full endpoint config by combining:
      - general info (datasource_name, dataset, source_type)
      - endpoint metadata from raw_config
    """

    keys_to_keep = ['api_path', 'arg_fields']
    config = {key: raw_config[key] for key in keys_to_keep if key in raw_config}

    dataset = dataset or f'airflow_{datasource_name}'
    bigquery_config = {'dataset': dataset}
    if raw_config.get('bigquery_table'):
        bigquery_config['table'] = raw_config['bigquery_table']
    config['bigquery_config'] = bigquery_config

    config['api'] = datasource_name

    raw_schema = raw_config.get('raw_schema')

    table_config = create_table_config(
        raw_schema,
        row_id=raw_config.get('row_id', []),
        partition=raw_config.get('partition'),
        context_fields=raw_config.get('context_fields')
    )
    config['table_config'] = table_config

    data_config_keys = ['source_type', 'fieldnames', 'delimiter', 'data_root']
    data_config = {key: raw_config[key] for key in data_config_keys if key in raw_config}
    if data_config:
        if data_config.get('source_type') == 'csv':
            data_config['fieldnames'] = data_config.get('fieldnames', list(raw_schema))
        config['data_config'] = data_config

    storage_config_keys = ['gcs_bucket', 'gcs_path']
    storage_config = {key: raw_config[key] for key in storage_config_keys if key in raw_config}
    if storage_config:
        config['storage_config'] = storage_config

    if raw_config.get('arg_fields'):
        config['api_arg_builder'] = create_arg_builder(
            raw_config['arg_fields'], api=datasource_name, api_path=raw_config['api_path']
        )

    log_string_list = [f'create_config: Created config for {datasource_name}.']
    for key, val in config.items():
        if isinstance(val, dict):
            log_string_item = f'{key}: \n\t{"\n\t".join([f'{k}: {v}' for k,v in val.items()])}'
        else:
            log_string_item = f'{key}: {val}'
        log_string_list.append(log_string_item)
    log_string = '\n------------\n'.join(log_string_list)
    logger.info(log_string)

    return config
