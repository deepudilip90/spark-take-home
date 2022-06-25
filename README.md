# spark-take-home solution
This project creates a data pipeline that 
 - Fetches data regarding users and messages from the API end points specified in the task instructions
 - Sanitises the data to remove PII information
 - Ingests the data to a MySQL database.
 - Creates some views for monitoring data anomalies inside the database

## Running the entire pipeline
The project and the ETL script can be run using the following command:
  _"**docker compose run app**"_
  
Once the above command is run, docker will initialise a MySQL service, and also execute the  etl.py script. The etl.py script will initialise a database by the name 'spark_dwh' within the MySQL server, then create appropriate tables and user accounts (details described below), and loads the data into the tables (PII is handled as well). Until the container is stopped, the database can be accessed at the address **localhost:3306** using the following credentials - **username: 'analyst', password: 'password'**. Note that the above credentials do not give access to any of the sensitive data tables! (This is part of protecting the PII data process and is explained in detail below). A detailed description of the pipeline and the associated components are given below!

Once the above command is run, docker will initialise a MySQL service, and also execute the  etl.py script. The etl.py script will initialise a database by the name 'spark_dwh' within the MySQL server, then create appropriate tables and user accounts (details described below), and loads the data into the tables (PII is handled as well). Until the container is stopped, the database can be accessed at the address **localhost:3306** using the following credentials - **username: 'analyst', password: 'password'**. Note that the above credentials do not give access to any of the sensitive data tables! A detailed description of the pipeline and the associated components are given below

## Important note while running!!
- I ran the above docker configuration using an M1 mac, hence had to add the line 'platform: linux/amd64' in the docker-compose.yml file. This might have to be altered when running on a different machine.
- During the inital run, it takes some time for the MySQL service to be up, and somehow I have not yet been able to figure out how to make the execution of etl.py script to wait until then. As a work around, I am using a method to repeatedly try to connect to the database at the beginning of the script (max 20 times, with a time delay between each successive try), using the method  _db_connector.check_db_availability()_. In my experience, the attempts fails during the initial few tries (this will be printed in the console), but after a few successive tries, the connection will be established and the rest of the script proceeds to execute. In case it doesn't connect even after 20 successive tries, try increasing the value of max_retries in the method call _db_connector.check_db_availability()_


Following are the basic components of this project
- A python script (etl.py) to run the main ETL process. This script fetches the data from the API using an API connector object (details below), performs data sanitisation for PII data removal and then writes the data to appropriate database tables in a MySQL database (created within a docker container) using a database connector object (details below)
- A docker file and docker compose file to set up the required services (MySQL database) and run the etl.py script

The diagram below gives an outline of the data flow:

![alt text](https://github.com/deepudilip90/spark-take-home/blob/main/Data-Flow.png?raw=true)

The rest of this readme explains the flow of the ETL process with detailed descriptions of each step

## Initial setup of database and tables
The first step the ETL process does is to set up a database by the name 'spark_dwh' using root credentials for the database. (The root credentials for the database are configured in the docker_compose.yml file). In order to achieve this, a database connector object is used, whose class is defined as MySqlDbConnector within the file connectors.py. This connector has the required functionalities to interact with the database within the scope of this project
After setting up the database, the following main tables are also created:
- users_raw: To store the user information (after masking / removing PII data)
- subscriptions_raw: To store the subscription data
- messages_raw: To store the messages meta data (actual message not added

In addition the following tables are also created to store the masking IDs of the PII information (city, zip and profession of user)
- sensitive_zipcode_ids: To store an 'id' for each unique value of zipcode encountered in the user data. It is this ID value that will be ingested into the users_raw table, rather than the actual zipcode value
- sensitive_city_ids: To store an 'id' for each unique value of city encountered in the user data. It is this ID value that will be ingested into the users_raw table, rather than the actual city value
- sensitive_profession_ids: To store an 'id' for each unique value of profession encountered in the user data. It is this ID value that will be ingested into the users_raw table, rather than the actual profession value

In addition to the above tables, a new user account by the name 'analyst' is created. This account has access to the tables 'users_raw', 'subscriptions_raw' and 'messages_raw', but do not have access to any of the tables with the sensitive data. This 'analyst' account is meant to be used by data analysts / scientists for furthre downstream analytics. The sensitive tables can be only accessed by the root user. (In reality, this coule be any service acccount, that is non-human, such as an account created for just production workflow.)

## Data Extraction from API
The data extraction from API is achieved by means of an API connector object defined using the class **SparkApiConnector** in the _connectors_._py_ file.
This connector object fetches the users and messages data from the corresponding end points within the etl.py script. The details of individual methods of this class can be found within the docstrings provided inside the code.

## Data Transformation - PII data handling
This is the most important step in the entire data flow process. The process of handling PII data is explained in detail in the following steps:

### PII data handling - broad idea and architecture
Certain direct PII informatino such as first name, last name and address of the users are removed from the incoming user data from API right away, as these do not help in any meaningful analytics downstream. However, as mentioned in the task description, the product owners would like to conduct analyses based on (but not limited to) city, email domain and profession. Hence these fields will not be removed, but masked. This is described below

### Masking of PII data fields city, zipcode, & profession
The masking is achieved by generating an 'id' value corresponding to each unique value for city / zipcode / profession. These id values and the corresponding actual values are stored in the tables with prefixes 'sensitive_' as mentioned in the section above on "Initial setup of database and tables". 

#### Access restriction to sensitive information
As mentioned in the same section, these sensitive tables can only be accessed by the root user, whereas the other tables are accessbile to the database user 'analyst' (which could be used analysts/ data scientists). In a real life set up, the sensitive tables can be made accessible to just a service account that executes the ETL process (rather than the root user as done in this project). The service account password to connect to the database can be stored in a secure location accessbile only to the ETL process. A few examples of such mechanism are password managers, or cloud services such as AWS secrets manager.
In this project, < the root password is just stored within the repository itself for sake of simplicity, but in real life this will NEVER be done! Passwords are never stored in Git repostories!!>


## Data loading
Coming back to the ETL process, the sensitive user data is masked using the ID values as explained above. The data is then written to the table 'users_raw'. The data for subscriptions are extracted from the user data, and stored to the table 'subscriptions_raw'. The messages data are sanitised to remove the actual messages and then stored to the table 'messages_raw'.

The MySqlDbConnector object is used to load the users, messages and subscriptions data into the corresponding database tables. The relevant methods to do this and their documentation can be found in the method docstrings of the class. For simplicity and safety, all the columns are ingested with the type 'VARCHAR' at the time of this load operation.

## Analysis queries
As specified in the task description the file sql_queries/sql_test.sql has queries to answer the following questions:
1. How many total messages are being sent every day?
2. Are there any users that did not receive any message? 
3. How many active subscriptions do we have today?
4. Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
5. Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?

Following are my comments regarding each of the questions
1. How many total messages are being sent everyday  - For each calendar date, the total messages sent are calculated
2. Are there any users that did not receive any messages - Yes, user_id 4 has not received any messages
3. How many active subscriptsion do we have today - 3. The assumption I have made is the subscription status should be 'active' and the subscription end_date should be in the future. It is seen that one of the subscription associated with user_id 3 does not satifsy the second condition - it has an end date in 2022-03-03 but is still shown as active. This is considered as a wrong / noisy data and hence excluded
4. Are there users sending messages without an active subscription?  - Yes, user_id 6 did not have a subscription at any point, yet was seen to be sending messages
5. Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it (SQL query)?:
With respect to question 5, there is an instance of inaccurate data which is mentioned in point 3. (subscription associated with user_id 3 having an end date in the past, but status showing as 'Active'). This is considered an anomlous condition

## Monitoring views
Data observability monitoring is an important aspect of creating robust pipelines. One of the anomalies is already explained in point 5 above. Another exapmle of anomaly is point 4, which is not exactly a problem with quality, but is still an anomaly.
Hence the last step of the ETL script is to create two views that helps monitor instances of the above 2 anomalies. These monitoring queries are stored in the path sql_queries/monitoring. Each file inside this folder is a monitoring query. The ETL process, (etl.py) at the very end creates views in the database based on each of these queries, so that we can directly ping the views for monitoring such anomalies

## Developer documentation
This section provides a high level overview of the different code modules and classes. Detailed information is provided via docstrings within the code. 
Apart from the docker related files, there are 3 modules - connectors.py, load.py, transform.py

### connectors.py 
Defines two classes MySqlDbConnector and SparkApiConnector for interacting with the database and API respectively. The class MySqlDbconnector provides public methods for the following:
- Insert records to a table in the database
- Fetch records from a table in a database based on certain constraints
- Write sensitive PII information in the database within access restricted tables, and create masking IDs for the same. The masking IDs will be later made public to external users.
- Create a view within the database based on a user specified query
- Check if database service is up and running 

The class SparkApiConnector provides public method for the following:
- Fetch user, messages and subscription data as JSON files from the
      corresponding API end point.

### load.py
This module provides functions to insert the users, subscriptions and messages data into the database. These functions only handle the insertions - the transformation and PII handling is done using functions in the module transform.py

### transform.py
This module contains the functions to do some cleaning and transformations of the raw data obtained from the API. 
In particular the functions to handle PII (masking / removal) is done with functions in this module.


