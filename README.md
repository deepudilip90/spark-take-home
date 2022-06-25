# spark-take-home solution
This project creates a data pipeline that 
 - Fetches data regarding users and messages from the API end points specified in the task instructions
 - Sanitises the data to remove PII information
 - Ingests the data to a MySQL database.

Following are the basic components of this project
- A python script (etl.py) to run the main ETL process
- A docker file and docker compose file to set up the required services (MySQL database)
The project and the ETL script can be run using the following command:
  _"docker compose run app"_
The rest of the readme explains the flow of the ETL process with detailed descriptions of each step
