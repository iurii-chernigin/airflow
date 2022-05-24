"""
    Here is DAG that writes information about TOP N locations by number of residents into a database
    Uses methods/components:
        * Custom RickMortyHook
        * Custom RickMortyTopLocationsOperator
        * PostgresHook, PostgresOperator
        * XCome for sharing the results of tasks 
        *  ShortCircuitOperator

    TODO: Check the existence of a table in the database 
    TODO: Check the existence of data in the table
    TODO: The DAG should be run every day
    TODO: The tasks should be restarted if it failed
"""
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from rickmorty.operators.rickmorty_top_locations_operator  import RickMortyTopLocationsOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'schedule_interval': '@daily'
}

@dag(
    max_active_runs=1,
    catchup=False,
    tags=['rickmorty', 'rickmorty-top-locations', 'rickmorty-api-analyze'],
    default_args=DEFAULT_ARGS
) 
def rickmorty_top_locations():
    pass
