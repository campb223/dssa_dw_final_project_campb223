from psycopg import connect, Connection
from psycopg.conninfo import make_conninfo
from configparser import ConfigParser

class PostgresClient():

    def __init__(self, host: str = None, port: int = None, user: str = None, password: str = None, dbname: str = None):
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
        self.user = user
        self.password = password
        self.dbname = dbname

    def connect_from_config(self, path: str, section: str, **kwargs) -> Connection:
        """
        Method that takes in the path and section of where a local copy of database connection parameters are. Files of this type should be included in the .gitignore file to prevent
        unauthorized credential access. The typical database.ini postgres file should look like:
            [postgresql]
            host = localhost
            port = 5432
            user = postgres
            password = pass
            dbname = dvdrental
        Args:
            path (str): The filepath with database connection parameters. 
            section (str): The file type to verify and read. 
        Returns:
            Connection: a new connection instance
        """

        conn_dict = {}
        config_parser = ConfigParser()

        # Read the configuration file
        config_parser.read(path)
        # If the file has the same section as passed in
        if config_parser.has_section(section):
            # Read the parameters specified. 
            config_params = config_parser.items(section)
            # Add each paramter after the = to a dictionary 
            for k, v in config_params:
                conn_dict[k] = v

        # Est a connection to the DB using the paramters read in. 
        conn = connect(conninfo=make_conninfo(**conn_dict), **kwargs)

        return conn

    def connect(self, **kwargs) -> Connection:
        """"
        A method to provide the database login information and return the connection (if valid) to the database. 
        Args:
            connectionList (List): A list of the login credentials to the database. Should match the pattern of:
                host = localhost
                port = 5432
                user = postgres
                password = pass
                dbname = dvdrental
        Returns:
            Connection: a new connection instance
        """
        
        conn = connect(conninfo=make_conninfo(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname, **kwargs))

        # checks if the connection is ok, it will throw an error if it is bad
        conn._check_connection_ok()

        return conn