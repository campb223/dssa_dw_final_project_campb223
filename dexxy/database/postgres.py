from psycopg import connect, Connection
from psycopg.conninfo import make_conninfo
from configparser import ConfigParser

class PostgresClient():
    
    def __init__(self, host: str = None, port: int=None, user: str=None, password: str=None, dbname: str=None):
        """
        Initilization of an object in PostgresClient(). 
        By default the host, port, user, password, and dbname are set to None
        For additional information related to connecting to PostgreSQL DB's:
            https://www.postgresqltutorial.com/postgresql-python/connect/

        Args:
            host (str, optional): Defaults to None.
            port (int, optional): Defaults to None.
            user (str, optional): Defaults to None.
            password (str, optional): Defaults to None.
            dbname (str, optional): Defaults to None.
        """
        self.host = host
        self.port = port
        self.username = user
        self.password = password
        self.dbname = dbname
        
    
    def connectToDatabase(self, path: str, section: str) -> Connection:
        """
        Method that takes in the path and section of where a local copy of database connection parameters are. Files of this type should be included in the .gitignore file to prevent
        unauthorized credential access. The typical database.ini postgres file should look like:
            [postgresql]
            host=localhost
            port=5432
            database=suppliers
            user=postgres
            password=SecurePas$1

        Args:
            path (str): The filepath with database connection parameters. 
            section (str): The file type to verify and read. 

        Returns:
            Connection: A connection to the database which can be used to query, read data, copy data, etc. 
        """
        connectionList = []
        configParser = ConfigParser()
        
        # Reading in the parameters from config file
        configParser.read(path)
        
        # If the file contains the matching section:section 
        if configParser.has_section(section):
            # Grabbing database login credentials and saving to params. 
            params = configParser.items(section)
            for k, v in params:
                connectionList.append(v)
        
        # Using the credentials we just read, we'll open a connection to that database.         
        client = PostgresClient(host=connectionList[0], port=connectionList[1], user=connectionList[2], password=connectionList[3], dbname=connectionList[4])
        connection = client.connect(connectionList)
        
        # IF we've made it here, the connection has been checked and should be clear to return. 
        return connection
    
    def connect(self, connectionList) -> Connection:
        """
        A method to provide the database login information and return the connection (if valid) to the database. 

        Args:
            connectionList (List): A list of the login credentials to the database. Should match the pattern of:
                host=localhost
                port=5432
                database=suppliers
                user=postgres
                password=SecurePas$1

        Returns:
            Connection: Checks the connection to verify it's stable. If it is, return the connection. 
        """
        connection = connect(conninfo=make_conninfo(
            host=connectionList[0],
            port=connectionList[1],
            user=connectionList[2],
            password=connectionList[3],
            dbname=connectionList[4]))
        
        # Check to verify connection is active. 
        connection._check_connection_ok()
        
        # If we're here, the connection is valid and we should be clear to return the connection. 
        return connection