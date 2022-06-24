from hologram import T
import requests
import mysql.connector
import json
import time
import pandas as pd

class MySqlDbConnector:
    def __init__(self,
                 host='localhost', 
                 port=3306,
                 username='root', 
                 password='p@ssw0rd1'):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._db_conn = None

    def _initialise_db_connection(self, database=None):
        """
        Convenience method to be set up a connection to the database.
        """
        if self._db_conn:
            return True
        max_retries = 10
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
                print('Successfully connected to database!')
                break
        
        if not db_conn:
            return False
    
        self._db_conn = db_conn
        return True

    def _close_db_connection(self):
        if self._db_conn:
            self._db_conn.close()
            self._db_conn = None
        return

    @staticmethod
    def _generate_insert_statement(record, table_name):
         field_string = ', '.join([str(key) for key in record.keys()])
         value_string = '", "'.join([value for _, value in record.items()])
         insert_string = (f'INSERT INTO {table_name} (' + 
                               field_string + ') VALUES ("' + 
                               value_string + '");')
         return insert_string

    @staticmethod
    def _generate_constraint_statement(record):
        condition_list = []
        for key, value in record.items():
            if isinstance(value, str):
                condition = str(key) + "='" + value + "'"
            else:
                condition = str(key) + "=" + str(value)
            condition_list.append(condition)
        
        constraint_statement = 'WHERE ' + ' AND '.join(condition_list)
        return constraint_statement
   
    def run_query(self, query, close_conn_after_exec=False, return_results=False, database='spark_dwh'):
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

    def fetch_records(self, table_name, fields=None, constraints_dict=None, database='spark_dwh'):
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
    
    # def _get_or_create_id(self, table_name, record_dict):
    #     _ , records = self.fetch_records(table_name, ['id'], record_dict)
    #     if not records:
    #         # do something
    #         query = f'SELECT MAX(id) FROM {table_name} '
    #         query_result = self.run_query(query, return_results=True)
    #         max_id = query_result[0][0]
    #         id = int(max_id) + 1
    #     else:
    #         if len(records) > 1:
    #             print('Error: Multiple IDs found for the given constraint!!')
    #             return None
    #         id = records[0][0]
    #     return id
    
    def _get_or_create_mask_id(self, table_name, record, database='spark_dwh'):
        # if table_name not in ['']:
        #     raise error
        _, result = self.fetch_records(table_name, ['id'], record, database=database)
        if not result:
            self.insert_record(table_name, record)
            _, result = self.fetch_records(table_name, ['id'], record, database=database)
        id = result[0][0]

        return id

    def initialise_db_and_create_tables(self):
        
        self._initialise_db_connection()
        self.run_query(query='CREATE DATABASE IF NOT EXISTS spark_dwh', close_conn_after_exec=True)

        self.run_query(query="""CREATE TABLE IF NOT EXISTS users
                        (created_at timestamp, updated_at timestamp, 
                        city_id INT, country VARCHAR(255), zipcode_id INT, 
                        email VARCHAR(255), birth_date timestamp,
                        gender VARCHAR(10), is_smoking BOOLEAN, profession_id INT,
                        income FLOAT)""", database='spark_dwh', close_conn_after_exec=True
        )

        self.run_query(query="""CREATE TABLE IF NOT EXISTS subscriptions
                        (created_at timestamp, start_date timestamp,
                        end_date timestamp, status VARCHAR(255),
                        amount FLOAT)""", database='spark_dwh', close_conn_after_exec=True
        )
        
        self.run_query(query="""CREATE TABLE IF NOT EXISTS messages
                        (created_at timestamp, receiver_id INT, 
                        id INT, sender_id INT)""", database='spark_dwh', close_conn_after_exec=True
        )

        self.run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_zipcode_ids
                        (id INT AUTO_INCREMENT, zipcode INT, PRIMARY KEY (id))""", 
                        database='spark_dwh', close_conn_after_exec=True
        )

        self.run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_city_ids
                        (id INT AUTO_INCREMENT, city VARCHAR(255), PRIMARY KEY (id))""", 
                        database='spark_dwh', close_conn_after_exec=True
        )

        self.run_query(query="""CREATE TABLE IF NOT EXISTS sensitive_profession_ids
                        (id INT AUTO_INCREMENT, profession VARCHAR(255), PRIMARY KEY (id))""", 
                        database='spark_dwh', close_conn_after_exec=True
        )
        # self._db_conn.commit()
        self._close_db_connection()

        print('database initialized!')


    
    def insert_record(self, table_name, record, fail_if_exists=True, database='spark_dwh'):
        self._initialise_db_connection()
        if fail_if_exists:
            # check if record exists:
            _, result = self.fetch_records(table_name=table_name, 
                                           fields=record.keys(), 
                                           constraints_dict=record,
                                           database=database)
            if result:
                print('Record already exists in database! Skipping insert')
                return False
        sql_query = self._generate_insert_statement(record=record, 
                                                    table_name=table_name)

        self.run_query(sql_query, database=database)
        self._db_conn.commit()
        return True




class SparkApiConnector:
    def __init__(self, headers=None):
        self._headers = headers

    @staticmethod
    def _check_api_reponse(response, error_log_message):
        
        if response.status_code != 200:
            print(error_log_message)
            print(f'Status code of API response is : {response.status_code}')
            return False
        return True
        
    def _fetch_data(self, end_point):
        try:
            response = requests.get(end_point, headers=self._headers)
        except Exception as err:
            print(f'Failed to fetch data from endpoint {end_point} due to reason below:')
            print(err)
            print(f'HTTP response status code is {response.status_code}')
            return []
        
        if not self._check_api_reponse(response=response, 
                                       error_log_message="Failed to fetch data from specified end point"):
            return []

        return response.json()
    
    def fetch_user_data(self, end_point=None):
        if not end_point:
            end_point = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
        
        return self._fetch_data(end_point=end_point)

    def fetch_messages_data(self, end_point=None):
        if not end_point:
            end_point = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages'
        
        return self._fetch_data(end_point=end_point)
        
        

if __name__ == '__main__':
    
    connector = MySqlDbConnector()
    new_record = {'zipcode': '2'}

    connector.initialise_db_and_create_tables()

    # url = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
    # api_connector = ApiConnector()
    # data = api_connector.fetch_data(url)



    
