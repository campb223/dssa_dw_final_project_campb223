import pandas as pd
from typing import List
from psycopg import Cursor
from pypika import PostgreSQLQuery
from pypika import Schema, Column, PostgreSQLQuery
from dexxy.common.tasks import Task
from dexxy.common.workflows import Pipeline
from dexxy.common.plotting import plot_dag
from dexxy.database.postgres import PostgresClient


################## Parameters ###################
# Neccessary for connecting to the database. 
# NOTE: if you're pulling this from github you will need to supply the database.ini file. 
# More information on the pattern can be found under dexxy/database/postgres.py

databaseConfig = "config/database.ini"
section = 'postgresql'
dw = Schema('dw')
dvd = Schema('public')


############## Table Definitions ################
# These are some generic builds for our star-schema. For visual reference refer to the star-schema.jpg in the Provided Materials. 
# These tables will be used with Pypika. Pypika "is a Python API for building SQL queries". For additional information on Pypika, visit the link below:
#     https://pypika.readthedocs.io/en/latest/

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
    Column('date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False)
)


################### Functions ####################
# These functions will be directly used in the ETL process to build a star schema. 
# They're mostly for connecting to the database, grabbing table data, and writing table data. 

def createCursor(path:str, section:str) -> Cursor:
    """ NEED TO COMBINE THESE
    This function uses the class generated in dexxy/database/postgres.py
    By passing the location to the database connection credentials and the database type, it will attempt to secure a conenction to the database and return the cursor (if succesful).
    
    The method will attempt to verify the connection before returning. 

    Args:
        path (str): The filepath to the database credentials. This filepath should not be shared with others as it should contain information about connecting to the database. 
        section (str): The type of database to connect to. For this project, I'm using PostgreSQL so section should be == "postgres"

    Returns:
        Cursor: A cursor connection to the database which can be used to read data, write data, etc. 
    """
    
    """Creates a Database Cursor for sending commands
    and queries to the connection instance
    Args:
        path (str): path to an ini file containing database params
        section (str): section name in the ini file containing pararms
    Returns:
        Cursor: A Cursor instance
    """
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor

def setSearchPath(cursor: Cursor) -> None:
    """
    Sets the default search path to the public schema to make our select queries use unqualified names (easier to work with). 

    Args:
        cursor (Cursor): A Cursor connection to the database. This will be used to connect, then set the search path. 
    """
    cursor.execute("SET search_path TO public;")
    return

def createSchema(cursor: Cursor, schemaName: str) -> Cursor:
    """
    If the schema provided in schemaName does NOT exist, it will be created in the database using the provided Cursor. 
    If the schema already exists, nothing is created. Returns Cursor. 

    Args:
        cursor (Cursor): A Cursor connection to the database.
        schemaName (str): A schemaName to create -- if it does not already exist. 

    Returns:
        Cursor: A Cursor connection to the database.
    """
    q = f"CREATE SCHEMA IF NOT EXISTS {schemaName};"
    cursor.execute(q)
    return cursor

def tearDown(cursor: Cursor) -> None:
    """
    Closes the connection to the database. 

    Args:
        cursor (Cursor): A Cursor connection to the database that will be closed. 
    """
    #cursor.execute("DROP SCHEMA dw CASCADE;")
    cursor.close()
    return
    
def createTable(cursor:Cursor, tableName:str, definition:tuple, primaryKey:str=None, foreignKeys:list=None, referenceTables:list=None) -> None: 
    """
    _summary_

    Args:
        cursor (Cursor): _description_
        tableName (str): _description_
        definition (tuple): _description_
        primaryKey (str, optional): _description_. Defaults to None.
        foreignKeys (list, optional): _description_. Defaults to None.
        referenceTables (list, optional): _description_. Defaults to None.
    """
    
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
    return     
    
def readData(cursor:Cursor, tableName:str, columns:tuple) -> pd.DataFrame:
    """
    Executes a query to selects Columns and rows from a Table using a cursor 
    
    Args:
        cursor (Cursor): A cursor instance
        tableName (str): name of the table to query
        columns (tuple): name of columns from the table to select
    
    Returns:
        pd.DataFrame: Returns results in a pandas dataframe
    """
    query = PostgreSQLQuery \
        .from_(tableName) \
        .select(*columns) \
        .get_sql()
    res = cursor.execute(query)
    data = res.fetchall()
    
    col_names = []
    
    for names in res.description:
        col_names.append(names[0])
    
    df = pd.DataFrame(data, columns=col_names)
    return df

def loadData(cursor: Cursor, df:pd.DataFrame, target:str):
    """
    Writes data to a table from a pandas dataframe
    
    Args:
        cursor (Cursor): A cusror instance to the database
        df (pd.DataFrame): pandas dataframe containing data to write 
        target (str): name of table for "INSERT" query
    """
    data = tuple(df.itertuples(index=False, name=None))
    query = PostgreSQLQuery \
        .into(target) \
        .insert(*data) \
        .get_sql()
    cursor.execute(query)
    return 

def buildDimCustomer(cust_df:pd.DataFrame) -> pd.DataFrame:
    """
    constructs the customer dimension object
    
    Args:
        cust_df (pd.DataFrame): dataframe from the raw customer table \
            that is usually the result of readTable
    
    Returns:
        pd.DataFrame: customer dimension object as a pandas dataframe
    """
    cust_df.rename(columns={'customer_id': 'sk_customer'}, inplace=True)
    cust_df['name'] = cust_df.first_name + " " + cust_df.last_name
    dim_customer = cust_df[['sk_customer', 'name', 'email']].copy()
    dim_customer.drop_duplicates(inplace=True)
    return dim_customer
    
def buildDimStaff(staff_df:pd.DataFrame) -> pd.DataFrame:
    """
    constructs the staff dimension object
    
    Args:
        staff_df (pd.DataFrame): dataframe from the raw staff table \
            that is usually the result of readTable
    
    Returns:
        pd.DataFrame: staff dimension object as a pandas dataframe
    """
    staff_df.rename(columns={'staff_id': 'sk_staff'}, inplace=True)
    staff_df['name'] = staff_df.first_name + " " + staff_df.last_name
    dim_staff = staff_df[['sk_staff', 'name', 'email']].copy()
    dim_staff.drop_duplicates(inplace=True)
    return dim_staff
    
def buildDimDates(dates_df:pd.DataFrame) -> pd.DataFrame:
    """
    constructs the dates dimension table
    
    Args:
        dates_df (pd.DataFrame): dataframe from the raw rental table \
            that is usually the result of readTable. The DVD rental \
            database does not have dates table so one is derived.
    
    Returns:
        pd.DataFrame: date dimension object as a pandas dataframe
    """
    dates_df = dates_df.copy()
    dates_df['sk_date'] = dates_df.rental_date.dt.strftime("%Y%m%d")
    dates_df['date'] = dates_df.rental_date.dt.date
    dates_df['quarter'] = dates_df.rental_date.dt.quarter
    dates_df['year'] = dates_df.rental_date.dt.year
    dates_df['month'] = dates_df.rental_date.dt.month
    dates_df['day'] = dates_df.rental_date.dt.day
    dim_dates = dates_df[['sk_date', 'date', 'quarter', 'year', 'month', 'day']].copy()
    dim_dates.drop_duplicates(inplace=True)
    return dim_dates

def buildDimStore(store_df:pd.DataFrame, 
    staff_df:pd.DataFrame, 
    address_df:pd.DataFrame,
    city_df:pd.DataFrame,
    country_df:pd.DataFrame) -> pd.DataFrame:
    """
    constructs the store dimension table
    
    Args:
        store_df (pd.DataFrame): dataframe from the raw store table
        staff_df (pd.DataFrame): dataframe from the raw staff table
        address_df (pd.DataFrame): dataframe from the raw address table
        city_df (pd.DataFrame): dataframe from the raw city table
        country_df (pd.DataFrame): dataframe from the raw country table
    
    Returns:
        pd.DataFrame: store dimension object as a pandas dataframe
    """
    
    staff_df.rename(columns={'manager_staff_id':'staff_id'}, inplace=True)
    staff_df['name'] = staff_df.first_name + " " + staff_df.last_name
    staff_df = staff_df[['staff_id', 'name']].copy()
    
    country_df = country_df[['country_id', 'country']].copy()
    city_df = city_df[['city_id', 'city', 'country_id']].copy()
    city_df = city_df.merge(country_df, how='inner', on='country_id')
    
    address_df = address_df[['address_id', 'address', 'district', 'city_id']].copy()
    address_df = address_df.merge(city_df, how='inner', on='city_id')
    address_df.rename(columns={'district': 'state'}, inplace=True)
    
    store_df.rename(columns={'manager_staff_id':'staff_id'}, inplace=True)
    store_df.rename(columns={'store_id': 'sk_store'}, inplace=True)
    store_df = store_df.merge(staff_df, how='inner', on='staff_id')
    store_df = store_df.merge(address_df, how='inner', on='address_id')
    store_df = store_df[['sk_store', 'name', 'address', 'city', 'state', 'country']].copy()
    return store_df

def buildDimFilm(film_df:pd.DataFrame, lang_df:pd.DataFrame) -> pd.DataFrame:
    """
    constructs the film dimension table
    
    Args:
        film_df (pd.DataFrame): dataframe from the raw film table
        lang_df (pd.DataFrame): dataframe from the raw language table
    
    Returns:
        pd.DataFrame: film dimension object as a pandas dataframe
    """
    
    film_df.rename(
        columns={'film_id': 'sk_film', 'rating':'rating_code', 'length':'film_duration'},
        inplace=True
        )
    
    lang_df.rename(
        columns={'name':'language'},
        inplace=True
        )
    
    film_df = film_df.merge(lang_df, how='inner', on='language_id')
    film_df = film_df[['sk_film', 'rating_code', 'film_duration', 'rental_duration', 'language', 'release_year', 'title']].copy()
    return film_df

def buildFactRental(rental_df:pd.DataFrame,
    inventory_df:pd.DataFrame,
    date_df:pd.DataFrame,
    film_df:pd.DataFrame,
    staff_df:pd.DataFrame,
    store_df:pd.DataFrame) -> pd.DataFrame:
    """
    _summary_
    
    Args:
        rental_df (pd.DataFrame): dataframe from the raw rental table
        inventory_df (pd.DataFrame): dataframe from the raw inventory table
        date_df (pd.DataFrame): dataframe containing dim table
        film_df (pd.DataFrame): dataframe containing dim film
        staff_df (pd.DataFrame): dataframe containing dim staff
        store_df (pd.DataFrame): dataframe containing dim store
    
    Returns:
        pd.DataFrame: fact rental object as a pandas dataframe
    """
    
    rental_df.rename(columns={'customer_id':'sk_customer', 'rental_date':'date'}, inplace=True)
    rental_df['date'] = rental_df.date.dt.date
    rental_df = rental_df.merge(date_df, how='inner', on='date')
    rental_df = rental_df.merge(inventory_df, how='inner', on='inventory_id')
    rental_df = rental_df.merge(film_df, how='inner', left_on='film_id', right_on='sk_film')
    
    rental_df = rental_df.merge(staff_df, how='inner', left_on='staff_id', right_on='sk_staff')
    rental_df = rental_df.merge(store_df, how='inner', on='name')
    
    rental_df = rental_df.groupby(
        ['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff']).agg(count_rentals=('rental_id','count')).reset_index()
    
    rental_df = rental_df[['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff', 'count_rentals']].copy()
    return rental_df

def removeDW(cursor):
    cursor.execute("DROP SCHEMA dw CASCADE;")
    cursor.close()
    return

def clearPastDBData():
    cursor = createCursor(databaseConfig, section)
    cursor.execute("DROP SCHEMA dw CASCADE;")
    cursor.close()
    
    
def main():
    
    clearPastDBData()
    
    initWorkflow = Pipeline(    
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
                kwargs={'tableName': dw.customer, 'primaryKey': 'sk_customer', 'definition':DIM_CUSTOMER},
                dependsOn=['createSchema'],
                name='createDimCustomer'),
            Task(createTable,
                kwargs={'tableName': dw.store, 'primaryKey': 'sk_store', 'definition':DIM_STORE},
                dependsOn=['createSchema'],
                name='createDimStore'),
            Task(createTable,
                kwargs={'tableName': dw.film, 'primaryKey': 'sk_film', 'definition':DIM_FILM},
                dependsOn=['createSchema'],
                name='createDimFilm'),
            Task(createTable,
                kwargs={'tableName': dw.staff, 'primaryKey': 'sk_staff', 'definition':DIM_STAFF},
                dependsOn=['createSchema'],
                name='createDimStaff'),
            Task(createTable,
                kwargs={'tableName': dw.date, 'primaryKey': 'sk_date', 'definition':DIM_DATE},
                dependsOn=['createSchema'],
                name='createDimDate'),
            Task(createTable,
                kwargs={
                    'tableName': dw.factRental, 'definition':FACT_RENTAL,
                    'foreignKeys': ['sk_customer', 'sk_store', 'sk_film', 'sk_staff', 'sk_date'],
                    'referenceTables': [dw.customer, dw.store, dw.film, dw.staff, dw.date]},
                dependsOn=['createSchema'],
                name='createFactRentals')
        ],
        type='default'
    )
    
    # Creates a DAG for extract, transform, and load to dim Customer
    cust_workflow = Pipeline(
        steps=[
            Task(readData,
                 kwargs={'tableName': dvd.customer,'columns': ('customer_id', 'first_name', 'last_name', 'email')},
                 dependsOn=['createCursor'],
                 name='extractCust'
                 ),
            Task(buildDimCustomer,
                 dependsOn=['extractCust'],
                 name='transfCust'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfCust', 'createDimCustomer'],
                 kwargs={'target': dw.customer},
                 name='loadCustomer',
                 skipValidation=True
                 )
            ]
        )
    
    # Creates a DAG for extract, transform, and load to dim Staff
    staff_workflow = Pipeline(
        steps=[
            Task(readData,
                kwargs={'tableName': dvd.staff,'columns': ('staff_id', 'first_name', 'last_name', 'email')},
                dependsOn=['createCursor'],
                name='extractStaff'
                ),
            Task(buildDimStaff,
                 dependsOn=['extractStaff'],
                 name='transfStaff'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfStaff', 'createDimStaff'],
                 kwargs={'target': dw.staff},
                 name='loadStaff',
                 skipValidation=True
                 )
            ]
        )
    
    # Creates a DAG for extract, transform, and load to dim Dates
    dates_workflow = Pipeline(
        steps=[
            Task(readData,
                 kwargs={'tableName': dvd.rental,'columns': ('rental_id', 'rental_date', 'inventory_id', 'staff_id', 'customer_id')},
                 dependsOn=['createCursor'],
                 name='extractDates'
                 ),
            Task(buildDimDates,
                 dependsOn=['extractDates'],
                 name='transfDates'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfDates', 'createDimDate'],
                 kwargs={'target': dw.date},
                 name='loadDates',
                 skipValidation=True
                 ),
            ]
        )

    # Creates a DAG for extract, transform, and load to dim Store
    store_workflow = Pipeline(
        steps=[
            Task(readData,
                 kwargs={'tableName': dvd.store,'columns': ('store_id','manager_staff_id', 'address_id')},
                 dependsOn=['createCursor'],
                 name='extractStore'
                 ),
            Task(readData,
                 kwargs={'tableName': dvd.address,'columns': ('address_id','address', 'city_id', 'district')},
                 dependsOn=['createCursor'],
                 name='extractAddress'
                 ),
            Task(readData,
                 kwargs={'tableName': dvd.city,'columns': ('city_id','city', 'country_id')},
                 dependsOn=['createCursor'],
                 name='extractCity'
                 ),
            Task(readData,
                 kwargs={'tableName': dvd.country,'columns': ('country_id','country')},
                 dependsOn=['createCursor'],
                 name='extractCountry'
                 ),
            Task(buildDimStore,
                 dependsOn=['extractStore', 'extractStaff', 'extractAddress', 'extractCity', 'extractCountry'],
                 name='transfStore'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfStore', 'createDimStore'],
                 kwargs={'target': dw.store},
                 name='loadStore',
                 skipValidation=True
                 ),
            ]
        )
    
    # Creates a DAG for extract, transform, and load to dim Film
    film_workflow = Pipeline(
        steps=[
            Task(readData,
                kwargs={'tableName': dvd.film,'columns': (
                    'film_id', 'rating', 'length', 'rental_duration', 'language_id','release_year', 'title')},
                dependsOn=['createCursor'],
                name='extractFilm'
                ),
            Task(readData,
                kwargs={'tableName': dvd.language,'columns': ('language_id', 'name')},
                dependsOn=['createCursor'],
                name='extractLanguage'
                ),
            Task(buildDimFilm,
                 dependsOn=['extractFilm', 'extractLanguage'],
                 name='transfFilm'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfFilm', 'createDimFilm'],
                 kwargs={'target': dw.film},
                 name='loadFilm',
                 skipValidation=True
                 )
        ]
    )
    
    # Creates a DAG for extract, transform, and load to Fact Rental
    fact_workflow = Pipeline(
        steps=[
            Task(readData,
                kwargs={'tableName': dvd.inventory,'columns': ('inventory_id', 'film_id', 'store_id')},
                dependsOn=['createCursor'],
                name='extractInventory'
                ),
            Task(buildFactRental,
                 dependsOn=['extractDates', 'extractInventory', 'transfDates', 'transfFilm', 'transfStaff', 'transfStore'],
                 name='transfFactRental'
                 ),
            Task(loadData,
                 dependsOn=['createCursor','transfFactRental', 'createFactRentals'],
                 kwargs={'target': dw.factRental},
                 name='loadFactRental',
                 skipValidation=True
                 )
        ]
    )
    
    # Creates a DAG for tear down tasks and closing out any open connections to the database
    teardown_workflow = Pipeline(
        steps =[
            Task(tearDown,
                dependsOn= [
                    'createCursor', 
                    film_workflow, 
                    store_workflow, 
                    dates_workflow, 
                    staff_workflow, 
                    cust_workflow, 
                    fact_workflow
                    ],
                name='tearDown',
                skipValidation=True)
            ]
        )
    
    
    # We merge all the above Pipelines into a single Pipeline containing all Tasks to be added to the DAG.
    workflow = Pipeline(
        steps=[
            initWorkflow,
            cust_workflow,
            staff_workflow,
            dates_workflow,
            store_workflow,
            film_workflow,
            fact_workflow,
            teardown_workflow
        ]
    )
    
    # ============================ COMPILATION ============================ #
    # This section composes the DAG from the provided Tasks 
    workflow.compose()
    
    # Optionally we can plot the DAG (this module needs work bewarned)
    #plot_dag(etl_workflow.dag, savefig=False, path='dag.png')
    #plot_dag(workflow.dag, savefig=True, path='dag.png')
    
    # Save the DAG so that it can be scheduled
    #workflow.dump(filename='dags/dvd_rental_workflow.pkl')

    # ============================ ENQUEUE ============================ #
    # This section uses the .collect() method which enqueues all tasks in the DAG
    # to a task FIFO queue in topological order 
    workflow.collect()

    # ============================ EXECUTION ============================ #
    # Runs a Maellin Workflow locally using a single worker
    # This option is good for debugging before presisting the workflow 
    # and submitting it to the scheduler.
    workflow.run()
    

if __name__ == '__main__':
    main()