
from connectors import MySqlDbConnector, SparkApiConnector
from load import insert_message_data, insert_subscription_data, insert_user_data
from transform import get_subscription_data, sanitize_sensitive_data_users, create_monitoring_views

def etl_main():
    """
    Main function performing all the steps such as extracting the data from the given
    API end points, sanitising the data to remove PII related information and load the 
    same to the database. In addition, some views are also created in the database 
    for data quality monitoring.
    """
    api_connector = SparkApiConnector()
    db_connector = MySqlDbConnector(username='root', password='p@ssw0rd1')
    
    print('Checking if database server is up!')
    db_connector.check_db_availability()

    db_connector.initialise_db_and_create_tables(drop_if_exists=True)

    api_users_data = api_connector.fetch_user_data()
    api_messages_data = api_connector.fetch_messages_data()
    api_subscription_data = get_subscription_data(api_users_data)

    api_users_data = sanitize_sensitive_data_users(api_users_data)
    if not insert_user_data(api_users_data):
        print('Error: One or more records could not be inserted successully in users table!')
    if not insert_subscription_data(api_subscription_data):
        print('Error: One or more records could not be inserted successully in subscriptions table!')
    if not insert_message_data(api_messages_data):
        print('Error: One or more records could not be inserted successully in messages table!')

    print('creating monitoring views..')
    create_monitoring_views()

    print("""All data ingested. please login to the mysql server running at localhost:3306 for accessing the data
             within the schame 'spark_dwh' """)

if __name__ == '__main__':
    etl_main()
