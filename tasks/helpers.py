from airflow.decorators import task
from collections import defaultdict
from itertools import chain

@task
def collect_xcom_for_expand(lists):
    return list(chain.from_iterable(lists))