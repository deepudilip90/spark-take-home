"""
This module contains all functions associated with inserting data from the 
API to the database, specifically the users, messages and subscription data.
"""
from datetime import datetime
from connectors import MySqlDbConnector

def _insert_data(table_name,
                 data,
                 include_update_time=True,
                 database='spark_dwh'):
    """
    Convenience function to import a list of data records in the form
    of dictionaries to a table using the available database connector.

    :param table_name: The name of the table to insert the data to.
    :param data: The list of records (in the form of dictionaries) to insert.
    :param include_update_time: Flag to specify if update time is to be included
                                while inserting the records.
    :param database: The name of the database schema in which the table is in.
    """
    db_connector = MySqlDbConnector()
    total_records = len(data)
    successful_inserts = 0
    failed_inserts = 0
    print(f'inserting records to table {table_name}')
    for idx, record in enumerate(data):
        if include_update_time:
            record['last_updated_at'] = str(datetime.now())
        record = {k: str(v) for k, v in record.items()}
        try:
            db_connector.insert_record(table_name=table_name, 
                                       record=record, 
                                       database=database)
            successful_inserts += 1
        except Exception as err:
            print(f'failed for record no {idx} due to error: {err}')
            failed_inserts += 1
    print(f'total records to insert: {total_records}')
    print(f'total successful inserts: {successful_inserts}')
    print(f'total failed records {failed_inserts}')
    return failed_inserts == 0

def insert_user_data(users_data):
    """
    Function to insert the users data coming from the API, after it has been
    sanitized to remove PII related information.

    :param users_data: A list of dictionaries specifiying user data records.
    """
    
    def check_if_pii_data_present(data_record):
        for field in ['city', 'zipcode', 'profession']:
            value = data_record.get(field)
            if value and not isinstance(value, int):
                return False
        return True

    records_to_insert = []
    for record in users_data:
        if not check_if_pii_data_present(record):
            print('PII values not removed from data record! skipping insert!')
            continue
        data_record = {'user_id': record.get('id'),
                       'created_at': record.get('createdAt'),
                       'updated_at': record.get('updatedAt'),
                       'city_id': record.get('city'),
                       'country': record.get('country'),
                       'zipcode_id': record.get('zipcode'),
                       'email': record.get('email'),
                       'birth_date': record.get('birthDate'),
                       'gender': record.get('profile', {}).get('gender'),
                       'is_smoking': record.get('profile', {}).get('isSmoking'),
                       'profession_id': record.get('profile', {}).get('profession'),
                       'income': record.get('profile', {}).get('income')}
        records_to_insert.append(data_record)
    return _insert_data('users_raw', records_to_insert)
    

def insert_subscription_data(subscription_data):
    """
    Function to insert the subscription data coming from the API.

    :param users_data: A list of dictionaries specifying subscriptoin
                      data records.
    """
    records_to_insert = []
    for record in subscription_data:
        data_record = {'user_id': record.get('user_id'),
                       'created_at': record.get('createdAt'),
                       'start_date': record.get('startDate'),
                       'end_date': record.get('endDate'),
                       'status': record.get('status'),
                       'amount': record.get('amount')
                       }
        records_to_insert.append(data_record)
    return _insert_data('subscriptions_raw', records_to_insert)

def insert_message_data(message_data):
    """
    Function to insert the messages data coming from the API. The message
    text is ignored while insert as this is sensitive information.

    :param message_data: A list of dictionaries specifying messages
                         data records.
    """
    records_to_insert = []
    for record in message_data:
        data_record = {'id': record.get('id'),
                       'created_at': record.get('createdAt'),
                       'receiver_id': record.get('receiverId'),
                       'sender_id': record.get('senderId')
                       }
        records_to_insert.append(data_record)
    return _insert_data('messages_raw', records_to_insert)
