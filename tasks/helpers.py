from airflow.decorators import task
from collections import defaultdict
from itertools import chain

@task
def collect_xcom_for_expand(lists):
    return list(chain.from_iterable(lists))


from airflow.decorators import task
import logging

@task
def reduce_xcoms(xcom_values):
    # Clean out None values
    xcom_values = [v for v in xcom_values if v is not None]

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

