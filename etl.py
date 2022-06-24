from distutils.util import subst_vars
from apispec import APISpec
import mysql.connector
import json
from os import path
from tabulate import tabulate
from connectors import MySqlDbConnector, SparkApiConnector
import time
import csv


def load_data(data, 
              table_name, 
              fields, 
              database='take_home', 
              mode='replace'):
    """
    Function to load data from a list of records (json style) into a specific
    table in the database.

    :param data: The data to be loaded to the database. The data is in the form
                 of list of dictionaries, with each item being one row-level
                 record.
    :param table_name: The name of the table to which to load the data.
    :param fields: The columns in the table to which the data should be loaded.
    :param database: The name of the database.
    :param mode: If 'replace' all existing records will be deleted and new 
                 records inserted. Otherwise, the records will be appended.
    """

    db_conn = _create_db_connection(database=database)

    cursor = db_conn.cursor()

    inserted_records = 0
    if mode == 'replace':
        cursor.execute(f'DELETE FROM {table_name} WHERE 1=1')
        print('deleted any existing records!')
    elif mode == 'append':
        pass
    else:
        print('Wrong mode specified!!')
        return False

    print(f'Inserting records into table {table_name}')
    for record in data:
        try:
            field_string = ', '.join([str(key) for key in record.keys()
                                      if key in fields])
            value_string = '", "'.join([value for key, value in record.items()
                                        if key in fields])
            insert_string = (f'INSERT INTO {table_name} (' + 
                               field_string + ') VALUES ("' + 
                               value_string + '");')
            # print(insert_string)
            cursor.execute(insert_string)
            inserted_records += 1
        except Exception as err:
            print(f'Failed to insert record {record} due to error {err}')
    db_conn.commit()
    print(f'{inserted_records} out of a total of {len(data)} records inserted')

    if inserted_records != len(data):
        print('Not all records could be loaded!!!')
        return False
    
    cursor.close()
    db_conn.close()
    return True

# def perform_analysis(sql_file_name, 
#                      output_columns,
#                      analysis_description='',
#                      database='take_home',
#                      sql_folder_path=None):
#     """
#     This function reads an sql query stored in a file and executes it against
#     the database. The results are printed to console and also written to a csv
#     file in the project working directory.

#     :param sql_file_name: The name of the SQL file containing the query to be
#                            executed.
#     :param output_columns: The names of the columns expected in the output 
#                             of the SQL query.
#     :param analysis_description: A description to be printed to console when
#                                  printing the results of the query.
#     :param database: The name of the database against which the query should be
#                      run.
#     :param sql_folder_path: The path to the folder in which the SQL files are 
#                             stored. If not specified a default path is used. 
#     """

#     db_conn = _create_db_connection(database=database)
#     cursor = db_conn.cursor()

#     if not sql_folder_path:
#         sql_folder_path = path.join(path.dirname(path.realpath(__file__)),
#                                     'sql_queries')
#     with open(path.join(sql_folder_path, sql_file_name), 'r') as f:
#         query = f.read()

#     cursor.execute(query)

#     result = cursor.fetchall() 

#     # result_df = pd.DataFrame(result, columns=output_columns)
#     if analysis_description:
#         print('Results for ' + analysis_description)
#     else:
#         print('Result for query in ' + sql_file_name)

#     print(tabulate(result, headers=output_columns))
    
#     print('\n')
#     output_base_path = path.dirname(path.realpath(__file__))
#     output_file_path = (output_base_path + '/result_' + 
#                         sql_file_name.replace('.sql', '') + '.csv')
   
#     _write_output_to_csv(result, output_file_path, output_columns)

#     db_conn.close()
#     cursor.close()


def sanitize_sensitive_data_users(users_data):
    sensitive_fields_remove = ['firstName', 'lastName', 'address']
    db_connector = MySqlDbConnector(username='root', password='p@ssw0rd1')
    sanitized_user_data = []
    for user_data in users_data.copy():
        user_data = {k: v for k, v in user_data.items() if k not in sensitive_fields_remove}
        city = user_data.get('city')
        zipcode = user_data.get('zipCode')
        email = user_data.get('email')
        profession = user_data.get('profile', {}).get('profession')
    
        if city:
            city_id = db_connector._get_or_create_mask_id('sensitive_city_ids', 
                                                          {'city': city})
            user_data['city'] = city_id
        if zipcode:
            zipcode_id = db_connector._get_or_create_mask_id('sensitive_zipcode_ids',
                                                             {'zipcode': zipcode})
            user_data['zipcode'] = zipcode_id
        if profession:
            profession_id = db_connector._get_or_create_mask_id('sensitive_profession_ids',
                                                                {'profession': profession})
            user_data['profile']['profession'] = profession_id
        if email and '@' in email:
            email_domain = email.split('@')[1]
            user_data['email'] = email_domain
        else:
            user_data['email'] = None
        sanitized_user_data.append(user_data)

    return sanitized_user_data

def insert_user_data(users_data):
    db_connector = MySqlDbConnector()
    #todo Add additional check for sensitive fields
    total_records = len(users_data)
    failed_records = 0
    for user_data in users_data:
        data_record = {'user_id': user_data.get('id'),
                       'created_at': user_data.get('createdAt'),
                       'updated_at': user_data.get('updatedAt'),
                       'city_id': user_data.get('city'),
                       'country': user_data.get('country'),
                       'zipcode_id': user_data.get('zipcode'),
                       'email': user_data.get('email'),
                       'birth_date': user_data.get('birthDate'),
                       'gender': user_data.get('profile', {}).get('gender'),
                       'is_smoking': user_data.get('profile', {}).get('isSmoking'),
                       'profession_id': user_data.get('profile', {}).get('profession'),
                       'income': user_data.get('profile', {}).get('income')}
        data_record = {k: str(v) for k, v in data_record.items()}
        try:
            db_connector.insert_record('users_raw', data_record)
        except Exception as err:
            print(f'failed for record due to error: {err}. Record printed below:')
            print(data_record)
            failed_records += 1
    
    print(f'total records to insert: {total_records}')
    print(f'total failed records {failed_records}')

def insert_subscription_data(subscription_data):


def get_subscription_data(users_data):
    all_subscription_data = []
    for user_data in users_data:
        user_id = user_data.get('id')
        subscriptions = user_data.get('subscription')
        if subscriptions:
            subscriptions  = [dict(item, **{'user_id': user_id}) for item in subscriptions]
            all_subscription_data.extend(subscriptions)
    
    return all_subscription_data


def main():
    """
    Main function performing all the steps such as reading from json file, 
    loading to database, running the SQL queries and writing the output.
    """
    api_connector = SparkApiConnector()
    db_connector = MySqlDbConnector(username='root', password='p@ssw0rd1')
    db_connector.initialise_db_and_create_tables(drop_if_exists=False)

    users_data = api_connector.fetch_user_data()
    messages_data = api_connector.fetch_messages_data()

    users_data = sanitize_sensitive_data_users(users_data)
    insert_user_data(users_data)
    subscription_data = get_subscription_data(users_data)




    



    

if __name__ == '__main__':
    main()
