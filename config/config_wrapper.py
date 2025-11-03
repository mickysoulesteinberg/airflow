import functools
from config.constants import CONFIG_SECTIONS
from utils.helpers import get_valid_kwargs

from config.logger import get_logger
logger = get_logger(__name__)

def with_config(sections=None):
    '''
    Decorator that merges selected sub-configs into function kwargs

    Use with functions that take a config, to allow optional sub-config arguments

    E.g. for a function that needs config settings for storage and data:
        @with_config(['storage_config', 'data_config'])
        def transform_gcs_for_bq(...):
            ... 
    '''
    sections = sections or CONFIG_SECTIONS.keys()
    logger.debug(f'with_config: sections={sections}')
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, config=None, **kwargs):
            config = config or {}
            resolved_kwargs = dict(kwargs)
            logger.trace(f'with_config(%s): sections=%s\nconfig=%s',
                         func.__name__,
                         sections,
                         '\n'.join([f'{key}: {value}' for key, value in config.items()])
                         )
            for section in sections:
                config_mapping = CONFIG_SECTIONS.get(section)
                logger.trace(f'with_config: config_mapping={config_mapping}')
                if not config_mapping:
                    logger.debug(f'with_config({func.__name__}): section {section} supplied, but not configured in global variable CONFIG_SECTIONS')
                    continue
                
                # Get the config from the supplied dictionary or overall config if available
                section_config = kwargs.get(section) or config.get(section) or {}
                if not section_config:
                    logger.debug(f'with_config: Noe section config for {section_config}')
                    # If there is no config, can just use the supplied kwargs, already in resolved_kwargs
                    continue
                
                logger.trace(f'with_config: section_config={section_config}')
                # Define any kwargs for this section config that weren't input as arguments to the function
                for kwarg_name, config_key in config_mapping.items():
                    if resolved_kwargs.get(kwarg_name) is None:
                        resolved_kwargs[kwarg_name] = section_config.get(config_key)
                        logger.micro(f'with_config: resolved kwarg {kwarg_name} to {resolved_kwargs[kwarg_name]}')
            
            return func(*args, **get_valid_kwargs(func, resolved_kwargs))

        return wrapper
    return decorator