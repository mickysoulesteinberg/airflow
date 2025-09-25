from airflow.decorators import dag
from airflow.utils.dates import days_ago
from task_groups.ingestion import api_fetch_and_load

API = 'tmdb'
YEARS = list(range(2000, 2005))
DAG_ID = 'tmdb_pipeline'

@dag(start_date=days_ago(1), schedule="@daily", catchup=False)
def tmdb_pipeline():
    # dag_status = StatusTable() #todo: set this up so can track status of dag at each step
    # Seed status table and retrieve any years which are not complete, feed into rest of pipeline

    year = YEARS[0] #TODO make it so we loop through here

    # Run Task Group to load movies to GCS & BigQuery, retain list of movie IDs for second run
    api_fetch_and_load(
        api = API,
        dag_id = DAG_ID,
        api_path = 'discover_movies',
        api_params = {'primary_release_year': year, 'sort_by': 'revenue.desc', 'page': 1}
    ) 

    # # Run Task Group to get credits for each movie, load to GCS & BigQuery
    # api_fetch_and_load(
    #     api = API,
    #     dag_id = DAG_ID,
    #     api_path = 
    # )

tmdb_pipeline()
