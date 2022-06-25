"""
This module contains the functions to do some cleaning and transformations of
the data coming from the API. In particular the functions to handle PII
(masking / removal) is done with functions in this module.
"""

from glob import glob
from connectors import MySqlDbConnector


def sanitize_sensitive_data_users(users_data, root_password):
    """
    This function processes the user data coming from the API to remove or
    mask the PII fields. In particular, the fields 'firstName', 'lastName' and
    'address' are removed from the data as they are direct PII. For the fields
    'city', 'zipcode', 'profession', the entries are replaced by a corresponding
    numeric value, generated and stored in the database. The actual ID to value
    mapping for these fields will be only accessible to the root user (or any
    other service user which is non-human) that will be executing this code.

    :param users_data: A list of dictionaries specifiying user data records 
                       as obtained directly from the API. This will contain
                       PII information.
    """
    sensitive_fields_remove = ['firstName', 'lastName', 'address']
    db_connector = MySqlDbConnector(username='root', password=root_password)
    sanitized_user_data = []
    for user_data in users_data.copy():
        user_data = {k: v for k, v in user_data.items() 
                     if k not in sensitive_fields_remove}
        city = user_data.get('city')
        zipcode = user_data.get('zipCode')
        email = user_data.get('email')
        profession = user_data.get('profile', {}).get('profession')
    
        if city:
            city_id = db_connector.get_or_create_mask_id('sensitive_city_ids',
                                                          {'city': city})
            user_data['city'] = city_id
        if zipcode:
            zipcode_id = db_connector.get_or_create_mask_id('sensitive_zipcode_ids',
                                                            {'zipcode': zipcode})
            user_data['zipcode'] = zipcode_id
        if profession:
            profession_id = db_connector.get_or_create_mask_id('sensitive_profession_ids',
                                                               {'profession': profession})
            user_data['profile']['profession'] = profession_id
        if email and '@' in email:
            email_domain = email.split('@')[1]
            user_data['email'] = email_domain
        else:
            user_data['email'] = None
        sanitized_user_data.append(user_data)

    return sanitized_user_data


def get_subscription_data(users_data):
    """
    Function to parse the subscription related data from the user data coming 
    from the API end point for users data.

    param: users_data: A list of dictionaries specifiying user data records as 
                       obtained from the API.
    """
    all_subscription_data = []
    for user_data in users_data:
        user_id = user_data.get('id')
        subscriptions = user_data.get('subscription')
        if subscriptions:
            subscriptions  = [dict(item, **{'user_id': user_id}) 
                              for item in subscriptions]
            all_subscription_data.extend(subscriptions)
    
    return all_subscription_data

def create_monitoring_views(query_base_path='sql_queries/monitoring'):
    """
    Create a set of views which are expected to be stored as
    .sql files in the path specified by parameter query_base_path. Each 
    file within the query_base_path is expected to be query to create a single
    monitoring view.

    param query_base_path: The path to the folder containing the queries to 
                            be executed.
    """
    db_connector = MySqlDbConnector()
    sql_files = glob(query_base_path + '/*')
    for file in sql_files:
        view_name = file.split('/')[-1].replace('.sql', '')
        query = open(file, 'r').read()
        db_connector.create_view(view_name=view_name, sql_query=query)