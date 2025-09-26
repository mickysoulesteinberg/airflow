from airflow.decorators import dag
from airflow.utils.dates import days_ago
from task_groups.ingestion import api_fetch_and_load

API = 'tmdb'
YEARS = [2000] #list(range(2000, 2005))
DAG_ID = 'tmdb_pipeline'

@dag(
        start_date=days_ago(1), 
        schedule="@daily", 
        catchup=False
)
def tmdb_pipeline():
    for year in YEARS:
    # Run Task Group to load movies to GCS & BigQuery, retain list of movie IDs for second run
        api_fetch_and_load(
            api = API,
            api_path = 'discover_movies',
            api_args = {
                'params': {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1},
                'call_id': f'year{year}' 
            }
        ) 

    # Run Task Group to get credits for each movie, load to GCS & BigQuery


tmdb_pipeline()
