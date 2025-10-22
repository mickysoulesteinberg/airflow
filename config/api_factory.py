from config.logger import get_logger
logger = get_logger(__name__)

BQ_METADATA_COL = 'airflow_metadata'

def create_arg_builder(arg_fields, api=None, api_path=None):
    """Returns a function that builds API request args dynamically."""
    logger.trace(f'Creating arg builder for {api}.{api_path}')

    def arg_builder(**kwargs):
        logger.debug(f'Building args for {api}.{api_path} with kwargs={kwargs}')
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

        logger.trace(f'Built args for {api_path}: {built_args}')
        return built_args

    return arg_builder


def create_table_config(raw_schema, row_id=None, arg_fields=None, source_type=None):
    """Creates BigQuery table config with schema + metadata."""
    logger.trace('Creating table config')
    if not raw_schema:
        raise ValueError('raw_schema must be provided')

    row_id = row_id or []
    arg_fields = arg_fields or {}

    # Cols from the raw data
    schema = []
    for name, dtype in raw_schema.items():
        col = {'name': name, 'type': dtype}
        if name in row_id:
            col['mode'] = 'REQUIRED'
        schema.append(col)

    # Metadata col
    schema.append({'name': BQ_METADATA_COL, 'type': 'JSON'})
    # for name, data in arg_fields.items():
    #     if data.get('data_type'):
    #         schema.append({'name': name, 'type': data['data_type']})

    table_config = {'schema': schema}
    if source_type:
        table_config['source_type'] = source_type
    if row_id:
        table_config['row_id'] = row_id

    return table_config


def create_config(datasource_name, raw_config, dataset=None, source_type='json'):
    """
    Create a full endpoint config by combining:
      - general info (datasource_name, dataset, source_type)
      - endpoint metadata from raw_config
    """
    dataset = dataset or f'airflow_{datasource_name}'
    api_path = raw_config['api_path']
    raw_schema = raw_config.get('api_schema', {})
    row_id = raw_config.get('row_id', [])
    arg_fields = raw_config.get('arg_fields', {})
    api_root = raw_config.get('api_root')

    config = {
        'api': datasource_name,
        'api_path': api_path,
        'bigquery_dataset': dataset,
        'table_config': create_table_config(
            raw_schema,
            row_id=row_id,
            arg_fields=arg_fields,
            source_type=source_type,
        )
    }

    if arg_fields:
        config['api_arg_builder'] = create_arg_builder(
            arg_fields, api=datasource_name, api_path=api_path
        )
        config['arg_fields'] = arg_fields
    if api_root:
        config['api_root'] = api_root
    

    return config
