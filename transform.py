from glob import glob
from connectors import MySqlDbConnector


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


def get_subscription_data(users_data):
    all_subscription_data = []
    for user_data in users_data:
        user_id = user_data.get('id')
        subscriptions = user_data.get('subscription')
        if subscriptions:
            subscriptions  = [dict(item, **{'user_id': user_id}) for item in subscriptions]
            all_subscription_data.extend(subscriptions)
    
    return all_subscription_data

def create_monitoring_views(query_base_path='sql_queries/monitoring'):
    db_connector = MySqlDbConnector()
    sql_files = glob(query_base_path + '/*')
    for file in sql_files:
        view_name = file.split('/')[-1].replace('.sql', '')
        query = open(file, 'r').read()
        db_connector.create_view(view_name=view_name, sql_query=query)