# DSSA 5102 Final Project

## Introduction
The intent of this project to implement an ETL (extract, transform, and load) pipeline from a database (DVD Rental) into a data warehouse. There are times when a linear data processing pipeline would be sufficient, however, we love a challenge so we must implement this as a DAG (Directed Acyclic Graph)
<br>
<br>
### Objectives
The main objective of this is to implement an ETL process in python to create a Star-Schema in a Data Warehouse. 
<br>
![Alt text](../../../../C:/Users/13045/dssa_dw_final_project_campb223/Provided%20Materials/star-schema.jpg)
<br>
There are a few processes that need to take place to create this model. The general process looks like:
    - Connect to the existing database - DVD Rental
    - Extract data from the database
    - Transform the data into the necessary variable types
    - Load the data into the Data Warehouse (dw)
<br>
#### Table Defintions <br>
__Fact Table: FACT_RENTAL__
- `sk_customer` is the `customer_id` from customer table 
- `sk_date` is `rental_date` from the rental table
- `sk_store` the `store_id` from the store table
- `sk_film` is the `film_id` from the film table
- `sk_staff` is the `id` from the staff table
- `count_rentals` A count of the total rentals grouped by all other fields in the table
<br>
__Dimension Table: STAFF__
- `sk_staff` is the `staff_id` field from the staff table
- `name` a concatenation of `first_name` and `last_name` from the staff table
- `email` is the `email` field from the staff table
<br>
__Dimension Table: CUSTOMER__
- `sk_customer` is the `customer_id` from customer table
- `name` is the concatenation of `first_name` & `last_name` from the customer table
- `email` is the customer's email 
<br>
__Dimension Table: DATE__
- `sk_date` is unique `rental_date` converted into an integer so it can be used as a primary key 
- `quarter` is a column formatted from `rental_date` for quarter of the year
- `year` is a column formatted from `rental_date` for year
- `month` is a column formatted from `rental_date` for month of the year
- `day` is a column formatted from `rental_date` for day of the month
<br>
__Dimension Table: STORE__ 
- `sk_store` the `store_id` from the store table
- `name` (manager of the store) is the concatenation of `first_name` and `last_name` from the staff table
- `address` is the `address` field from the address table
- `city` is the `city` field from the city table
- `state` is the `district` field from the address table
- `country` is the `country field from the country table
<br>
__Dimension Table: FILM__
- `sk_film` is the `film_id` from the film table
- `rating_code` is the `rating` field from the film table
- `film_duration` is the `length` field from the film table
- `rental_duration` is the `rental_duration` from the film table
- `language` is the `name` field from the language table
- `release_year` is the `release_year` from the film table
- `title` is the `title` field from the film table
<br>
<br>
## How The Project Is Organized:
### Project Structure
*   `.config` - This folder contains configuration files. Included is a sample `database.ini` to show how to connect to a PostreSQL server. 
*   `dexxy` - This is the source code folder containing all application code and modules.
*   `docs` - Documentation about the source code. 
*   `Provided Materials` - These were the documents provided to us during the project. 
*   `.gitignore` - In this file, you can include any code, folders, config files, etc. that you do NOT want uploaded to github. (Think of files like database.ini that include connection passwords.)
*   `LICENSE` - Open source GNU license markdown
*   `README` - Markdown file describing the project, objetives, and how to use. 
*   `requirements.txt` - list of python libraries to install with `pip`. These are necessary for code execution. 
<br>
## How Did I Develop My Python Modules?
<br>
__Tasks__ - 
<br>
__Queue__ - 
<br>
__Scheduler__ - 
<br>
__Worker__ - 
<br>
__Executor__ - 
<br>
__DAG__ - 
<br>
__Workflow__ - 
<br>
<br>
## How Should You Organize your `main.py`
<br>
