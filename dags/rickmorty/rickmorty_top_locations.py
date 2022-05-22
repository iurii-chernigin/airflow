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