"""
This is the main python script for extracting data from the API, 
cleaning, transforming the data to remove / mask PII information 
load the data to the MySQL database.
"""

from connectors import  MySqlDbConnector, SparkApiConnector
from load import insert_message_data, insert_subscription_data, insert_user_data
from transform import (get_subscription_data, 
                       sanitize_sensitive_data_users, 
                       create_monitoring_views)


def get_root_password():
    """
    Example method to get root password. In real life, this can be an API call
    to a secrets manager application.
    """
    return open('root_credentials.txt', 'r').read()

def etl_main():
    """
    Main function performing all the steps such as extracting the data from the 
    given API end points, sanitising the data to remove PII related information 
    and load the same to the database. In addition, some views are also created 
    in the database for data quality monitoring.
    """
    api_connector = SparkApiConnector()
    root_password = get_root_password()
    db_connector = MySqlDbConnector(username='root', password=root_password)
    
    print('Checking if database server is up!')
    db_connector.check_db_availability(max_retries=20)

    db_connector.initialise_db_and_create_tables(drop_if_exists=False)

    api_users_data = api_connector.fetch_user_data()
    api_messages_data = api_connector.fetch_messages_data()
    api_subscription_data = get_subscription_data(api_users_data)

    api_users_data = sanitize_sensitive_data_users(api_users_data, 
                                                   root_password=root_password)
    if not insert_user_data(api_users_data, 'root', root_password):
        print('Error: One or more records could not be inserted \
               successully in users table!')
    if not insert_subscription_data(api_subscription_data, 'root', root_password):
        print('Error: One or more records could not be inserted \
               successully in subscriptions table!')
    if not insert_message_data(api_messages_data,  'root', root_password):
        print('Error: One or more records could not be inserted \
               successully in messages table!')

    print('creating monitoring views..')
    create_monitoring_views('root', root_password)

    print("""All data ingested. please login to the mysql server running at
             localhost:3306 for accessing the data
             within the schema 'spark_dwh'
    """)

if __name__ == '__main__':
    etl_main()
