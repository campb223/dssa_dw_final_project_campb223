from configparser import ConfigParser
import psycopg2
import os

# Guide for connecting to DB
# https://www.postgresqltutorial.com/postgresql-python/connect/

def config(filename='config/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
       
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    if conn is not None:
        print("Successfully connected to the database.")
        return cur

def closeConnection(cursor):
    # close the communication with the PostgreSQL
    cursor.close()
    return