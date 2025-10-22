import inspect, logging

def get_valid_kwargs(fn,  kwargs):
    '''
    Filter a context dict so only kwargs valid for the function `fn` are returned.

    Useful for Airflow tasks where extra kwargs are present (e.g. conf, ti)

    Args:
        fn (callable): Function you plan to call with kwargs
        context (dict): All kwargs sent to the wrapper function
    
    Returns:
        dict: Filtered kwargs safe to pass to fn
    '''
    sig = inspect.signature(fn)
    valid_keys = sig.parameters.keys()
    filtered_kwargs = {k: v for k, v in  kwargs.items() if k in valid_keys}

    # Log dropped keywords in case of unintential lost kwargs
    dropped_kwargs = set( kwargs) - set(filtered_kwargs)
    if dropped_kwargs:
        logging.debug(f'[helpers.get_valid_kwargs] Dropped invalid kwargs for {fn.__name__}: {dropped_kwargs} ')
    
    return filtered_kwargs