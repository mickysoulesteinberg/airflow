from airflow.decorators import task
from itertools import chain
import logging

@task
def reduce_xcoms(xcom_values):
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

