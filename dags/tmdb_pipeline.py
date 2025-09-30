from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import tasks.ingestion as ingestion_tasks
from itertools import chain
import logging

logger = logging.getLogger(__name__)
YEARS = [2003, 2004]

@dag(
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False,
    params = {'years': YEARS}
)
def tmdb_pipeline():

    @task
    def extract_years_param():
        context = get_current_context()
        return context['params']['years']

    @task_group
    def ingest_top_movies_for_year(year):
        api = 'tmdb'
        api_path = 'discover_movies'

        api_args = {
            'params': {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1},
        }

        # Get the storage info to store data and later retrieve it
        gcs_uri = ingestion_tasks.get_storage_data(
            api = api,
            api_path = api_path,
            call_params = {'year': year}
        )

        # Performa the API Call and save the data to GCS
        movie_ids = ingestion_tasks.api_fetch(
            api = api,
            api_path = api_path,
            api_args = api_args,
            gcs_uri = gcs_uri,
            return_data = {'movie_ids': 'results[].id'}
        )

        return {'gcs_uri': gcs_uri, 'movie_ids': movie_ids}

    @task()
    def concat_results(results):
        return list(chain.from_iterable(r['movie_ids'] for r in results))

    # Task group to load top movies to BigQuery
    @task
    def load_top_movies_to_bigquery(gcs_uris):
        logger.warning('Inserting Top movies to Bigquery')
        logger.info(f'GCS URIs: {gcs_uris}')
        return gcs_uris
    
    # Task group to get cast for top movies and load to GCS
    @task
    def fetch_cast_for_movie(movie_ids):
        logger.warning('Getting Cast Data')
        logger.info(f'Movie IDs: {movie_ids}')
        return movie_ids

    top_movies = ingest_top_movies_for_year.expand(year = extract_years_param())
    gcs_uris = top_movies['gcs_uri']
    movie_ids = concat_results(top_movies['movie_ids'])
    nothing1 = load_top_movies_to_bigquery(gcs_uris)
    nothing2 = fetch_cast_for_movie(movie_ids)
    return
    


    # @task
    # def get_list(results, name):
    #     if isinstance(results, str):
    #         return [r[name] for r in results]
    #     if isinstance(results, list):
    #         return list(chain.from_iterable(r[name] for r in results))
    
    # @task
    # def concat_and_split(results):
    #     gcs_uris = [r['gcs_uri'] for r in results]
    #     movie_ids = list(chain.from_iterable( r['data'] for r in results))
    #     return gcs_uris #, movie_ids

    # @task
    # def print_results(results):
    #     logger.warning(f'Results {results}')
    
    # top_movies = top_movies_to_gcs_by_year.expand(year=extract_years_param())
    # # gcs_uris, movie_ids = concat_and_split(top_movies)
    # # gcs_uris = concat_and_split(top_movies)
    # gcs_uris = get_list(top_movies, 'gcs_uri')
    # movie_ids = get_list(top_movies, 'movie_ids')

    # print_results(gcs_uris)
    # print_results(movie_ids)

    


    # Run Task Group to get credits for each movie, load to GCS & BigQuery


tmdb_pipeline()
