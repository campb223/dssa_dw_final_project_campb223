# DSSA 5102 Final Project

## Introduction
The intent of this project to implement an ETL (extract, transform, and load) pipeline from a database (dvdrental) into a data warehouse. There are times when a linear data processing pipeline would be sufficient, however, we love a challenge so we must implement this as a DAG (Directed Acyclic Graph)
<br>
<br>
### Objectives
The main objective of this is to implement an ETL process in python to create a Star-Schema in a Data Warehouse. 
<br>
<br>
There are a few processes that need to take place to create this model. The general process looks like:
    - Connect to the existing database - dvdrental
    - Extract data from the database
    - Transform the data into the necessary variable types
    - Load the data into the Data Warehouse (dw)
<br>
### Table Defintions 
<b>Fact Table: FACT_RENTAL</b> <br>
- `sk_customer` is the `customer_id` from customer table
- `sk_date` is `rental_date` from the rental table
- `sk_store` the `store_id` from the store table
- `sk_film` is the `film_id` from the film table
- `sk_staff` is the `id` from the staff table
- `count_rentals` A count of the total rentals grouped by all other fields in the table

<b>Dimension Table: STAFF</b> 
- `sk_staff` is the `staff_id` field from the staff table
- `name` a concatenation of `first_name` and `last_name` from the staff table
- `email` is the `email` field from the staff table

<b>Dimension Table: CUSTOMER</b> 
- `sk_customer` is the `customer_id` from customer table 
- `name` is the concatenation of `first_name` & `last_name` from the customer table 
- `email` is the customer's email  

<b>Dimension Table: DATE</b> 
- `sk_date` is unique `rental_date` converted into an integer so it can be used as a primary key  
- `quarter` is a column formatted from `rental_date` for quarter of the year 
- `year` is a column formatted from `rental_date` for year 
- `month` is a column formatted from `rental_date` for month of the year
- `day` is a column formatted from `rental_date` for day of the month 

<b>Dimension Table: STORE</b> 
- `sk_store` the `store_id` from the store table 
- `name` (manager of the store) is the concatenation of `first_name` and `last_name` from the staff table 
- `address` is the `address` field from the address table 
- `city` is the `city` field from the city table 
- `state` is the `district` field from the address table 
- `country` is the `country field from the country table 

<b>Dimension Table: FILM</b> 
- `sk_film` is the `film_id` from the film table 
- `rating_code` is the `rating` field from the film table 
- `film_duration` is the `length` field from the film table 
- `rental_duration` is the `rental_duration` from the film table 
- `language` is the `name` field from the language table
- `release_year` is the `release_year` from the film table 
- `title` is the `title` field from the film table 

## How The Project Is Organized:
### Project Structure
*   `config` - This folder contains configuration files. Included is a sample `database.ini` to show how to connect to a PostreSQL server. 
*   `dags` - Within this folder will be DAGs that can be run on a schedule.
*   `dexxy` - This is the source code folder containing all application code and modules. 
*   `Provided Materials` - These were the documents provided to us during the project.  
*   `Samples` - Genearl bits about how to use tasks, pipelines, etc. 
*   `.gitignore` - In this file, you can include any code, folders, config files, etc. that you do NOT want uploaded to github. (Think of files like database.ini that include connection passwords.) 
*   `LICENSE` - Open source GNU license markdown 
*   `README` - General overview of the project objective, structure, etc.  
*   `requirements.txt` - list of python libraries to install with `pip`. These are necessary for code execution.  

## How Did I Develop My Python Modules? 
`Tasks` - 
<br>
`Queue` -  A First In - First Out (FIFO) design pattern. My Queue is called a `warehouse`. Currently there is only one type that is initiated -- Default = ThreadSafeQueue
<br>
`Scheduler` - 
<br>
`Worker` - 
<br>
`Executor` - 
<br>
`DAG`- 
<br>
`Workflow` - 
<br>
<br>
## How To Organize `main.py` 
*   As always in Python list your imports at the top of the file. 
*   Next list your connection parameters
*   Define your table definitions
*   Define your functions for building tables, reading data, loading data, etc. 
*   Then inside main setup your Pipelines/Tasks for connecting to the DB, extracting, transforming, loading, and teardown. 
*   Lastly in main; Compose, Enqueue, and Exectute. 

When you put it all together it should look something like this:

```
import pandas as pd

################## Parameters ###################
databaseConfig = "config/database.ini"
section = 'postgresql'
dw = Schema('dw')
dvd = Schema('public')

############## Table Definitions ################
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False)
)

################### Functions ####################
def createCursor(path:str, section:str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor

def main():
    setup = Pipeline(
        steps=[
            Task(createCursor,
                kwargs={'path': databaseConfig, 'section': section},
                dependsOn=None,
                name='createCursor'),
        ],
        type='default'
    )

    teardown = Pipeline(
        steps =[
            Task(tearDown,
                dependsOn= [
                    'createCursor',
                name='tearDown',
                skipValidation=True)
            ]
        )

    workflow = Pipeline(
        steps=[
            setup,
            extract,
            transform,
            load,
            teardown
        ]
    )

     # ============================ COMPILATION ============================ #
    workflow.compose()

    # ============================ ENQUEUE ============================ #
    workflow.collect()

    # ============================ EXECUTION ============================ #
    workflow.run()
```