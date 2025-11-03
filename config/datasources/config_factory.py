from config.constants import CONFIG_SECTIONS

from config.logger import get_logger
logger = get_logger(__name__)


BQ_METADATA_COL = 'airflow_metadata'
BQ_TIMESTAMP_COL = 'last_updated'

def get_sub_config(raw_config, config_section):
    sub_config = {}

    # Get any values that come directly from the raw_config
    for arg_name, config_key in CONFIG_SECTIONS[config_section].items():
        raw_value = raw_config.get(arg_name)
        if raw_value:
            sub_config[config_key] = raw_value
    return sub_config


def create_data_config(raw_config):
    logger.debug(f'create_data_config: Creating data config')
    data_config = get_sub_config(raw_config, 'data_config')
    logger.trace(f'create_data_config: Initial config={data_config}')
    if not data_config.get('fields'):
        data_config['fields'] = [key for key in raw_config['raw_schema']]
    return data_config

def create_bigquery_config(datasource_name, raw_config):
    '''Create the bigquery config for a datasource'''
    logger.debug(f'create_bigquery_config: Creating BQ config for {datasource_name}')
    
    bigquery_config = get_sub_config(raw_config, 'bigquery_config')
    logger.trace(f'create_bigquery_config: Initial config={bigquery_config}')

    # Set default dataset
    if not bigquery_config.get('dataset'):
        bigquery_config['dataset'] = f'airflow_{datasource_name}'

    # Create initial Row ID
    row_id = raw_config.get('row_id', [])

    # Create Initial Schema
    if bigquery_config.get('schema'):
        schema = bigquery_config['schema']
    elif raw_config.get('raw_schema'):
        schema = []
        for name, dtype in raw_config['raw_schema'].items():
            col = {'name': name, 'type': dtype}
            if name in row_id:
                col['mode'] = 'REQUIRED'
                col['ROW_ID'] = True
            schema.append(col)
    else:
        raise ValueError('Must provide raw_schema or bigquery_schema in raw_config')
    logger.trace(f'create_bigquery_config: Initial schema={schema}')

    # Add any information from Context Fields
    context_fields = raw_config.get('context_fields', {})
    for field, attributes in context_fields.items():
        dtype = attributes.get('type', 'STRING')
        col = {'name': field, 'type': dtype, 'source': 'CONTEXT'}
        if attributes.get('row_id'):
            col['mode'] = 'REQUIRED'
            col['ROW_ID'] = True
        schema.append(col)
    
    # Cols for all workflows
    schema.append({'name': BQ_METADATA_COL, 'type': 'JSON'})
    schema.append({'name': BQ_TIMESTAMP_COL, 'type': 'TIMESTAMP', 'PARTITION': 'DAY'})

    # Update schema
    bigquery_config['schema'] = schema

    logger.trace(f'create_bigquery_config: Config={bigquery_config}')
    return bigquery_config


def create_arg_builder(arg_fields, name=None, path=None):
    '''Returns a function that builds the API request arguments dynamically'''
    logger.trace(f'Creating arg builder for ')

    def arg_builder(**kwargs):
        logger.trace(f'arg_builder: Creating args for {name}.{path}')
        params, path_vars, call_id = {}, {}, ''

        for field, settings in arg_fields.items():
            value = None
            if field in kwargs:
                value = kwargs[field]
                call_id += f'{field}{value}'
            elif 'default' in settings:
                value = settings['default']
            else:
                continue

            param = settings.get('param')
            if param:
                params[param] = value
            
            path_var = settings.get('path_var')
            if path_var:
                path_vars[path_var] = value
            
        built_args = {}
        if params:
            built_args['params'] = params
        if path_vars:
            built_args['path_vars'] = path_vars
        if call_id:
            built_args['call_id'] = call_id
        
        logger.trace(f'arg_builder: Built args for {path}: {built_args}')
        return built_args

    return arg_builder

def create_api_config(datasource_name, raw_config):
    '''Create the api config for a datasource'''
    logger.debug(f'create_api_config: Creating API Config for {datasource_name}')

    api_config = get_sub_config(raw_config, 'api_config')
    logger.trace(f'create_api_config: Initial config={api_config}')

    # Set values
    name = api_config.get('name', datasource_name)
    api_config['name'] = name
    path = api_config.get('api_path')
    arg_fields = api_config.get('arg_fields')

    if arg_fields:
        api_config['arg_builder'] = create_arg_builder(arg_fields, name, path)

    name = api_config.get('name')
    if not name:
        name = datasource_name
        api_config['name'] = datasource_name
    
    # Set arg builder
    arg_fields = api_config.get('arg_fields')
    if arg_fields:
        api_config['arg_builder'] = create_arg_builder(arg_fields)
    
    return api_config

def create_config(datasource_name, raw_config):
    '''Create a full endpoint config that conforms to CONFIG_SECTIONS'''
    logger.info(f'create_config: Building config for {datasource_name}')
    
    config = {'name': datasource_name}

    # Bigquery Config
    config['bigquery_config'] = create_bigquery_config(datasource_name, raw_config)

    # API Config
    if raw_config.get('api_path'):
        config['api_config'] = create_api_config(datasource_name, raw_config)
    else:
        logger.debug(f'create_config: No api_path provided, skipping API config')
    
    # Data Config
    data_config = create_data_config(raw_config)
    if data_config:
        config['data_config'] = data_config
    else:
        logger.debug(f'create_config: No data config provided, skipping data config')

    # Storage Config
    storage_config = get_sub_config(raw_config, 'storage_config')
    if storage_config:
        config['storage_config'] = storage_config
    else:
        logger.debug(f'create_config: No storage config provided, skipping storage config')
    return config