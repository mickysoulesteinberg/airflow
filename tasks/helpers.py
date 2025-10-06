from airflow.decorators import task
from collections import defaultdict
from itertools import chain

@task
def collect_xcom_for_expand(lists):
    return list(chain.from_iterable(lists))


from airflow.decorators import task
import logging

@task
def reduce_xcoms(xcom_values, reducer_func_name='flatten'):
    """
    Generic reduce task to merge or aggregate mapped XCom results.
    Handles flattening, summing, or set-union behaviors.

    Parameters
    ----------
    xcom_values : list
        List of mapped task outputs (each element may be list, int, str, etc.)
    reducer_func_name : str, optional
        'flatten' → merge nested lists safely (default)
        'sum'     → sum numeric values
        'union'   → combine unique elements (like a set)
    """
    logging.info(f"Reducing {len(xcom_values)} XCom values using mode '{reducer_func_name}'")

    # Clean out None values
    xcom_values = [v for v in xcom_values if v is not None]
    # TODO temporarily cutting the list for dev
    xcom_values = xcom_values[:2]

    if reducer_func_name == 'flatten':
        flattened = []
        for v in xcom_values:
            if isinstance(v, (list, tuple)):
                flattened.extend(v)
            elif isinstance(v, str):
                flattened.append(v)
            else:
                flattened.append(v)
        logging.info(f"Flattened result: {flattened}")
        return flattened

    elif reducer_func_name == 'sum':
        return sum(xcom_values)

    elif reducer_func_name == 'union':
        merged = set()
        for v in xcom_values:
            if isinstance(v, (list, tuple, set)):
                merged |= set(v)
            else:
                merged.add(v)
        return list(merged)

    else:
        raise ValueError(f"Unknown reducer_func_name '{reducer_func_name}'")
