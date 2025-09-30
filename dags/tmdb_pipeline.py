from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from task_groups.ingestion import api_to_gcs
from itertools import chain
import logging

logger = logging.getLogger(__name__)

API = 'tmdb'
YEARS = [2002] #list(range(2000, 2005))
DAG_ID = 'tmdb_pipeline'

@dag(
    start_date=days_ago(1), 
    schedule="@daily", 
    catchup=False,
    params = {'years': [2003, 2004]}
)
def tmdb_pipeline():

    @task
    def get_years():
        context = get_current_context()
        return context['params']['years']
    
    # Run Task Group to load movies to GCS & BigQuery, retain list of movie IDs for second run
    @task_group
    def top_movies_to_gcs(year):
        return api_to_gcs(
            api = API,
            api_path = 'discover_movies',
            api_args = {
                'params': {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1},
                'call_params': {'year': year}
            },
            x_com_data = {
                'movie_ids': 'results[].id'
            }
        )
    
    @task
    def get_list(results, name):
        if isinstance(results, str):
            return [r[name] for r in results]
        if isinstance(results, list):
            return list(chain.from_iterable(r[name] for r in results))
    
    @task
    def concat_and_split(results):
        gcs_uris = [r['gcs_uri'] for r in results]
        movie_ids = list(chain.from_iterable( r['data'] for r in results))
        return gcs_uris #, movie_ids

    @task
    def print_results(results):
        logger.warning(f'Results {results}')
    
    top_movies = top_movies_to_gcs.expand(year=get_years())
    # gcs_uris, movie_ids = concat_and_split(top_movies)
    # gcs_uris = concat_and_split(top_movies)
    gcs_uris = get_list(top_movies, 'gcs_uri')
    movie_ids = get_list(top_movies, 'movie_ids')

    print_results(gcs_uris)
    print_results(movie_ids)

    


    # Run Task Group to get credits for each movie, load to GCS & BigQuery


tmdb_pipeline()
