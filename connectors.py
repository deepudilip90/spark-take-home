"""
This module defines the connectors required to interact with the API as well as
the database. In particular, this file defines two classes -
-  the SparkApiConnector for interacting with the API end points and fetch
   relevant data
- the MysqlDbConnector for interacting with the MySQL database within which
   all the data fetched from the API will be stored.
"""
import time
import requests
import mysql.connector


class MySqlDbConnector:
    """
    This class is used to create a connector object that is useful for
    interacting with the database. In particular, this class provides
    following functionalities via the its public methods:
    - Insert records to a table in the database
    - Fetch records from a table in a database based on certain constraints
    - Write sensitive PII information in the database within access restricted
      tables, and create masking IDs for the same.
      The masking IDs will be later made public to external users.
    - Create a view within the database based on a user specified query

    """
    def __init__(self,
                 username='root',
                 password='p@ssw0rd1',
                 host='mysqldbprod',
                 port=3306,
):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._db_conn = None

    def _initialise_db_connection(self, 
                                  database=None,
                                  max_retries=10, 
                                  log_success=False,
                                  exit_if_unavailable=False):
        """
        Convenience method to be set up a connection to the database.

        :param database: The name of the database to which the connection is to
                         be set up. If not provided, a generic conenction to 
                         server is created.
        :param max_retries: The number of times to attempt to connect to the 
                            database before declaring failure
        :param log_sucess: Bool value to print a log message if the database
                           connection could be successfully established.
        :param exit_if_unavailable: Flag to indicate whether to quit the 
                                    calling program if database cannot be 
                                    connected to
        """
        if self._db_conn:
            return True
        tries = 0

        conn_args = {'host': self._host,
                    'port': self._port,
                    'user': self._username,
                    'password': self._password,
                    'database': database}
        conn_args = {k: v for k, v in conn_args.items() if v}

        while tries <= max_retries:
            try:
                db_conn = mysql.connector.connect(**conn_args)
            except Exception as error:
                print(f'Cannot connect to database due to error {error}. Retrying...')
                tries += 1
                db_conn = None
                time.sleep(3)
                continue
            if db_conn:
                if log_success:
                    print('Successfully connected to database!')
                break
        
        if not db_conn:
            print('Unable to connect to database after multiple attempts!')
            if exit_if_unavailable:
                print('Exiting program!')
                exit(1)
        
        self._db_conn = db_conn

    def _close_db_connection(self):
        """
        Convenience method to close the existing database connection.
        """
        if self._db_conn:
            self._db_conn.close()
            self._db_conn = None

    def check_db_availability(self):
        """
        Method to check if the database is available and can be connected to.
        This is especially required when running apps via docker compose, since
        MySQL server takes some time to start up, and sometimes the python 
        script does not wait for the service to be up before beginnig execution.
        """
        print('Attempting to connect to database server')
        self._initialise_db_connection(max_retries=20, 
                                       log_success=True, 
                                       exit_if_unavailable=True)
        self._close_db_connection()


    @staticmethod
    def _generate_insert_statement(record, table_name):

        """
        Generate a SQL insert query for a given record and a table name

        :param record: The record to be inserted to the table.
        :param table_name: The name of the table to insert the record to.
        """

        field_string = ', '.join([str(key) for key in record.keys()])
        value_string = '", "'.join([value for _, value in record.items()])
        insert_string = (f'INSERT INTO {table_name} (' + 
                               field_string + ') VALUES ("' + 
                               value_string + '");')
        return insert_string

    @staticmethod
    def _generate_constraint_statement(record):
        """
        Generate a 'WHERE' constraint clause for an SQL query based on values
        specified in a dictionary. By default, all key-values in the dictionary
        are combined using an 'AND' condition for creating the constraint
        statement.

        :param record: The dictionary of key-value pairs specifying the
                       constraint condition.
        """
        condition_list = []
        for key, value in record.items():
            if isinstance(value, str):
                condition = str(key) + "='" + value + "'"
            else:
                condition = str(key) + "=" + str(value)
            condition_list.append(condition)
        
        constraint_statement = 'WHERE ' + ' AND '.join(condition_list)
        return constraint_statement
   
    def _run_query(self,
                   query, 
                   close_conn_after_exec=False, 
                   return_results=False, 
                   database='spark_dwh'):
        """
        Method to execute and SQL query inside the database.

        :param query: The query to execute.
        :param close_conn_after_exec: Bool to specify if the database connection
                                      should be closed after executing the 
                                      query.
        :param return_results: Bool to specify if a resulting set of records
                               are expected after running the query.
        :param database: The name of the database on which to execute the query.
        """
        self._initialise_db_connection(database=database)
        cursor = self._db_conn.cursor()
        cursor.execute(query)
        if return_results:
            results = cursor.fetchall()
            cursor.close()
        else:
            results = None
        self._db_conn.commit()
        
        if close_conn_after_exec:
            self._close_db_connection()
        
        return results

    def fetch_records(self,
                      table_name, 
                      fields=None, 
                      constraints_dict=None,
                      database='spark_dwh'):
        """
        Method to fetch a list of records from a database table based on a
        specified set of constraints.

        :param table_name: The name of the table from which to fetch the records.
        :param fields: The columns to retrieve from the table.
        :param constraints_dict: A dictionary specifying the constraints to be
                                 used when fetching the records.
        :param database: The name of the database where the table is located.
        """
        self._initialise_db_connection(database=database)
        cursor = self._db_conn.cursor()
        if fields:
            fields_statement = ', '.join(fields)
            query = f'SELECT ' + fields_statement + f' FROM {table_name} '
        else:
            query = f'SELECT * FROM {table_name} '
        
        if constraints_dict:
            constraint_statement = self._generate_constraint_statement(constraints_dict)
            query += constraint_statement
        cursor.execute(query)

        values = cursor.fetchall()
        fields = [desc[0] for desc in cursor.description]
        cursor.close()

        return fields, values
    
    def get_or_create_mask_id(self, table_name, record, database='spark_dwh'):
        """
        This method fetches a 'masking' id corresponding to record 
        (specified as a dictionary) from the table storing all the
        masking ids. If the record is a new entry and does not have a
        corresponding masking ID in the table, a new masking ID will be 
        created. At the backend, this is achieved using a table that has an 
        auto-increment 'id' field, which creates a new ID for every new record 
        inserted.
        """
        if self._username != 'root':
            print("""Warning: This method requires permissions to access to
                     tables only root / service users! """)
            return None

        _, result = self.fetch_records(table_name,
                                      ['id'],
                                       record,
                                       database=database)
        if not result:
            self.insert_record(table_name, record)
            _, result = self.fetch_records(table_name,
                                           ['id'],
                                           record,
                                           database=database)
        id = result[0][0]

        return id

    def initialise_db_and_create_tables(self, drop_if_exists=False):
        """
        Method to initialise the database and create required tables at the 
        start of the ETL process. This method will be called in the main ETL
        process as a root user, and hence all necessary initial set up will be
        executed. Following steps are done
        1) Create database spark_dwh
        2) Create tables for users, messages, subscriptions and also the tables
           for storing sensitive PII information 
        3) Create a new database user having access to only non-sensitive tables
           (non-PII related tables). This user account will be providded to the 
           analysts 
        """
        if drop_if_exists:
            print('dropping existing database and associated tables & users!')
            self._run_query(query='DROP TABLE IF EXISTS users_raw',
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS subscriptions_raw',
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS messages_raw', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS sensitive_zipcode_ids', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS sensitive_city_ids', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS sensitive_profession_ids', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP TABLE IF EXISTS spark_dwh', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP USER IF EXISTS analyst', 
                            close_conn_after_exec=True)
            self._run_query(query='DROP DATABASE IF EXISTS spark_dwh', 
                            database=None, 
                            close_conn_after_exec=True)

        if self._username != 'root':
            print('DB initialisation can be done only as root user!')
            return False
        
        self._initialise_db_connection()
        self._run_query(query='CREATE DATABASE IF NOT EXISTS spark_dwh',
                        close_conn_after_exec=True)

        self._run_query(query="""CREATE TABLE IF NOT EXISTS users_raw
                        (user_id VARCHAR(255), created_at VARCHAR(255),
                         updated_at VARCHAR(255), city_id VARCHAR(255), 
                         country VARCHAR(255), zipcode_id VARCHAR(255), 
                         email VARCHAR(255), birth_date VARCHAR(255),
                        gender VARCHAR(10), is_smoking VARCHAR(255), 
                        profession_id VARCHAR(255), income VARCHAR(255), 
                        last_updated_at VARCHAR(255))""", 
                        database='spark_dwh', 
                        close_conn_after_exec=True
        )

        self._run_query(query="""CREATE TABLE IF NOT EXISTS subscriptions_raw
                        (user_id VARCHAR(255), created_at VARCHAR(255), 
                         start_date VARCHAR(255), end_date VARCHAR(255),
                         status VARCHAR(255), amount VARCHAR(255), 
                         last_updated_at VARCHAR(255))""", 
                        database='spark_dwh', 
                        close_conn_after_exec=True
        )
        
        self._run_query(query="""CREATE TABLE IF NOT EXISTS messages_raw
                        (created_at VARCHAR(255), receiver_id VARCHAR(255), 
                        id VARCHAR(255), sender_id VARCHAR(255), 
                        last_updated_at VARCHAR(255))""",
                        database='spark_dwh', 
                        close_conn_after_exec=True
        )

        self._run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_zipcode_ids
                        (id INT AUTO_INCREMENT, zipcode VARCHAR(255),
                        last_updated_at VARCHAR(255), PRIMARY KEY (id))""",
                        database='spark_dwh',
                        close_conn_after_exec=True
        )

        self._run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_city_ids
                        (id INT AUTO_INCREMENT, city VARCHAR(255), 
                         last_updated_at VARCHAR(255), PRIMARY KEY (id))""",
                        database='spark_dwh',
                        close_conn_after_exec=True
        )

        self._run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_profession_ids
                        (id INT AUTO_INCREMENT, profession VARCHAR(255), 
                         last_updated_at VARCHAR(255), PRIMARY KEY (id))""", 
                        database='spark_dwh',
                        close_conn_after_exec=True
        )
        
        self._run_query(query="""CREATE USER IF NOT EXISTS 'analyst' 
                                 IDENTIFIED BY 'password'""")
        self._run_query(query="""GRANT ALL PRIVILEGES ON 
                                 spark_dwh.users_raw to 'analyst'""")
        self._run_query(query="""GRANT ALL PRIVILEGES ON 
                                 spark_dwh.subscriptions_raw to 'analyst'""")
        self._run_query(query="""GRANT ALL PRIVILEGES ON
                                 spark_dwh.messages_raw to 'analyst'""")

        self._close_db_connection()

        print('database initialized!')


    def insert_record(self,
                      table_name,
                      record,
                      fail_if_exists=True,
                      database='spark_dwh'):
        """
        Method to insert a record to a database table. 
        
        :param table_name: The name of the table to insert the record to.
        :param record: The record to insert as a dictionary.
        :param fail_if_exits: Check if the record already exists in the table
                              and fail if found.
        :param database: The name of the database in which the table resides.
        """
        if fail_if_exists:
            # check if record exists:
            constraints_dict = {k: v for k,v in record.items() 
                                if k != 'last_updated_at'}
            _, result = self.fetch_records(table_name=table_name, 
                                           fields=record.keys(), 
                                           constraints_dict=constraints_dict,
                                           database=database)
            if result:
                print('Record already exists in database! Skipping insert')
                return False
        sql_query = self._generate_insert_statement(record=record, 
                                                    table_name=table_name)

        self._run_query(sql_query, database=database)
        self._db_conn.commit()
    
    def create_view(self, view_name, sql_query):
        """
        Method to create a view by passing the SQL query for creating the same.
        
        :param view_name: The name of the view to be created.
        :param sql_query: The query to be used for creating the view.
        """
        query = f'CREATE OR REPLACE VIEW  {view_name} AS (' + sql_query + ');'
        self._run_query(query=query)

        

class SparkApiConnector:
    """
    This class is used to create a connector object that is useful for
    interacting with the API end points. In particular, this class provides
    following functionalities via the its public methods:
    - Fetch user, messages and subscription data as JSON files from the
      corresponding API end point.
    """
    def __init__(self, headers=None):
        self._headers = headers

    @staticmethod
    def _check_api_reponse(response, error_log_message):
        """
        Convenience method to check the status of response recieved from the 
        API.

        :param response: The API response object.
        :param error_log_message: The log message to print if the status of
                                  response received is not 200.
        """
        if response.status_code != 200:
            print(error_log_message)
            print(f'Status code of API response is : {response.status_code}')
            return False
        return True
        
    def _fetch_data(self, end_point):
        """
        Convenience method to fetch data from an API end point.

        :param end_point: The end point from which to receive the API response.
        """
        try:
            response = requests.get(end_point, headers=self._headers)
        except Exception as err:
            print(f'''Failed to fetch data from endpoint
                  {end_point} due to reason below:''')
            print(err)
            print(f'HTTP response status code is {response.status_code}')
            return []
  
        if not self._check_api_reponse(response=response,
                                       error_log_message="""Failed to fetch
                                       data from specified end point"""):
            return []

        return response.json()

    def fetch_user_data(self, end_point=None):
        """
        Method to fetch data from the users API end point.

        :param end_point: Use this parameter if required to change the default 
                          endpoint.
        """
        if not end_point:
            end_point = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
        
        return self._fetch_data(end_point=end_point)

    def fetch_messages_data(self, end_point=None):
        """
        Method to fetch data from the messages API end point.

        :param end_point: Use this parameter if required to change the default 
                          endpoint.
        """
        if not end_point:
            end_point = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages'
        
        return self._fetch_data(end_point=end_point)
        
        

if __name__ == '__main__':
    ROOT_PASSWORD = 'p@ssw0rd1'
    ANALYST_PASSWORD = 'password'
    connector = MySqlDbConnector(username='root', password=ROOT_PASSWORD)

    connector.initialise_db_and_create_tables()

    # url = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
    # api_connector = ApiConnector()
    # data = api_connector.fetch_data(url)



    
