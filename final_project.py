import pandas as pd
from psycopg import Connection, Cursor
from pypika import PostgreSQLQuery
from pypika import Query, Schema, Column
from dexxy.common.clients.tasks import Task
from dexxy.common.clients.workflows import Pipeline
from dexxy.common.clients.postgres import PostgresClient
from typing import List


### Parameters
databaseConfig = ".config\.postgres"
section = 'postgresql'
dw = Schema('dssa')


### Table Definitions
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False)
)

DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_STORE = (
    Column('sk_store', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('address', 'VARCHAR(100)', False),
    Column('city', 'VARCHAR(100)', False),
    Column('state', 'VARCHAR(100)', False),
    Column('country', 'VARCHAR(100)', False)
)

DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(100)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'VARCHAR(100)', False),
    Column('release_year', 'INT', False),
    Column('title', 'VARCHAR(255)', False)
)

DIM_DATE = (
    Column('sk_date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False)
)



### Functions
def createCursor(path:str, section:str) -> Cursor:
    client = PostgresClient()
    connection = client.connect_from_config(path, section, autocommit=True)
    cursor = connection.cursor()
    return cursor

def createSchema(cursor: Cursor, schemaName:str) -> Cursor:
    q = f"Create Scema If It Does Not Exist {schemaName};"
    cursor.execute(q)
    return cursor

def createTable(
    cursor: Cursor,
    tableName: str,
    definition:tuple,
    primaryKey: str=None,
    foreignKeys: list=None,
    referenceTables: list=None) -> Cursor:
    
    ddl = PostgreSQLQuery \
        .create_table(tableName) \
        .if_not_exists() \
        .columns(*definition)
        
    if primaryKey is not None:
        ddl = ddl.primary_key(primaryKey)
        
    if foreignKeys is not None:
        for idx, key in enumerate(foreignKeys):
            ddl.foreign_key(
                columns=key,
                reference_table = referenceTables[idx],
                reference_columns = key
            )
            
    ddl = ddl.get_sql()
    
    cursor.execute(ddl)
    return cursor

def tearDown(cursor: Cursor) -> None:
    cursor.execute("DROP SCHEMA DSSA CASCADE;")
    cursor.close()
    return

def main():
    ### Workflows
    
    setupDwWorkflow = Pipeline(
        steps=[
            Task(createCursor,
                kwargs={'path': databaseConfig, 'section': section},
                dependsOn=None,
                name='createCursor'),
            Task(createSchema,
                kwargs={"schemaName": dw._name},
                dependsOn=['createCursor'],
                name='createSchema'),
            Task(createTable,
                kwargs={'tableName': dw.dimCustomer, 'primaryKey': 'sk_customer', 'definition':DIM_CUSTOMER},
                dependsOn=['createSchema'],
                name='createDimCustomer'),
            Task(createTable,
                kwargs={'tableName': dw.dimStore, 'primaryKey': 'sk_store', 'definition':DIM_STORE},
                dependsOn=['createSchema'],
                name='createDimStore'),
            Task(createTable,
                kwargs={'tableName': dw.dimFilm, 'primaryKey': 'sk_film', 'definition':DIM_FILM},
                dependsOn=['createSchema'],
                name='createDimFilm'),
            Task(createTable,
                kwargs={'tableName': dw.dimStaff, 'primaryKey': 'sk_staff', 'definition':DIM_STAFF},
                dependsOn=['createSchema'],
                name='createDimStaff'),
            Task(createTable,
                kwargs={'tableName': dw.dimDate, 'primaryKey': 'sk_date', 'definition':DIM_DATE},
                dependsOn=['createSchema'],
                name='createDimDate'),
            Task(createTable,
                kwargs={
                    'tableName': dw.factRental, 'definition':FACT_RENTAL,
                    'foreignKeys': ['sk_customer', 'sk_store', 'sk_film', 'sk_staff', 'sk_date'],
                    'referenceTables': [dw.dimCustomer, dw.dimStore, dw.dimFilm, dw.dimStaff, dw.dimDate]},
                dependsOn=['createSchema'],
                name='createFactRentals')
        ],
        type='default'
    )
    
    
    
    ### Compilation 
    setupDwWorkflow.compose()
    
    
    ### Enqueue
    setupDwWorkflow.collect()
    
    
    ### Execution
    # Local Execution
    setupDwWorkflow.run()

    # To Schedule DAG 
    #setupDwWorkflow.submit()    
    
if __name__ == '__main__':
    main()