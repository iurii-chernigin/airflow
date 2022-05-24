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
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from rickmorty.operators.rickmorty_top_locations_operator  import RickMortyTopLocationsOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'schedule_interval': '@daily',
    'start_date': pendulum.datetime(2022, 5, 23, tz='UTC')
}

@dag(
    max_active_runs=1,
    catchup=False,
    tags=['rickmorty', 'rickmorty-top-locations', 'rickmorty-api-analyze'],
    default_args=DEFAULT_ARGS
) 
def rickmorty_top_locations():
    
    csv_data_path = '/tmp/ram_top_locations.csv'
    pg_connection = 'greenplum_students'
    pg_table = 'public.j_chernigin_8_ram_location'
    pg_table_columns = '(id, name, type, dimension, resident_cnt)'

    top_locations_searching = RickMortyTopLocationsOperator(
        task_id='top_locations_searching',
        top_count=3,
        result_csv_path=csv_data_path
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=pg_connection,
        sql='sql/rickmorty_create_table.sql'
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id=pg_connection,
        sql='sql/rickmorty_truncate_table.sql'
    )

    @task
    def copy_to_db_from_csv():
        pg_hook = PostgresHook(pg_connection)
        query = 'COPY {pg_table} {pg_table_columns} from STDING {options}'.format(
            pg_table=pg_table,
            pg_table_columns=pg_table_columns,
            options="DELIMITER ',' CSV HEADER"
        )
        pg_hook.copy_expert(query, csv_data_path)

    remove_csv_from_tmp = BashOperator(
        task_id='remove_csv_from_tmp',
        bash_command='rm {}'.format(csv_data_path)
    )

    top_locations_searching >> create_table >> truncate_table >> copy_to_db_from_csv() >> remove_csv_from_tmp

dag = rickmorty_top_locations()
