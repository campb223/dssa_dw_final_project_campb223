import pandas as pd
from psycopg import Cursor
from pypika import PostgreSQLQuery
from pypika import Schema, Column, PostgreSQLQuery
from dexxy.common.clients.tasks import Task
from dexxy.common.clients.workflows import Pipeline
#from dexxy.database.postgres2 import PostgresClient
from database.postgres import connect
from typing import List

dw = Schema('dssa')
dvd = Schema('public')

def createCursor():
    return connect()

def createSchema(cursor, schemaName):
    q = f"CREATE SCHEMA IF NOT EXISTS {schemaName};"
    cursor.execute(q)
    return cursor

def createTable(cursor, tableName, definition, primaryKey, foreignKeys, referenceTables):
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

def readData(cursor, tableName, columns):
    """Executes a query to selects Columns and rows from a Table using a cursor 
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

def loadData(cursor, df, target):
    """Writes data to a table from a pandas dataframe
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

def buildDimCustomer(cust_df):
    """constructs the customer dimension object
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
    
def buildDimStaff(staff_df):
    """constructs the staff dimension object
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
    
def buildDimDates(dates_df):
    """constructs the dates dimension table
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

def buildDimStore(store_df, staff_df, address_df, city_df, country_df):
    """constructs the store dimension table
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

def buildDimFilm(film_df, lang_df):
    """constructs the film dimension table
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

def buildFactRental(rental_df, inventory_df, date_df, film_df, staff_df, store_df):
    """_summary_
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

def tearDown(cursor):
    #cursor.execute("DROP SCHEMA DSSA CASCADE;")
    cursor.close()
    return

### Table Definitions
# For more information on the table design, refer the the star-schema.jpg image in Provided Materials. 
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

def main():
    #cursor = createCursor()
    
    initWorkflow = Pipeline(
        steps=[
            Task(createCursor,
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
    
    
    
    # Creates a DAG for tear down tasks and closing out any open connections to the database
    teardown_workflow = Pipeline(
        steps =[
            Task(tearDown,
                dependsOn= [
                    'createCursor', 
                    #film_workflow, 
                    #store_workflow, 
                    #dates_workflow, 
                    #staff_workflow, 
                    #cust_workflow, 
                    #fact_workflow
                    ],
                name='tearDown',
                skipValidation=True)
            ]
        )
    
    
    # We merge all the above Pipelines into a single Pipeline containing all Tasks to be added to the DAG.
    workflow = Pipeline(
        steps=[
            initWorkflow,
            #cust_workflow,
            #staff_workflow,
            #dates_workflow,
            #store_workflow,
            #film_workflow,
            #fact_workflow,
            teardown_workflow
        ]
    )
    
    ### Compilation 
    workflow.compose()
    
    
    ### Enqueue
    workflow.collect()
    
    
    ### Execution
    workflow.run()
    
    
if __name__ == '__main__':
    main()