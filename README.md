# etl-pipeline-large-data 
This project is a Flask-based microservice that integrates Kafka for message queuing, MySQL for data storage, and Flask-Mail for email notifications. The microservice consumes Kafka messages, processes CSV files, performs data cleaning and aggregation, and stores the data in a MySQL database.

**Tech stack:**
Python
Flask
Kafka
MYSQL

All required libraries and dependencies are listed in requirements.txt

ETL - 
Reads CSV data in chunks to handle large datasets efficiently.
Each chunk is cleaned, transformed, and loaded into the database.
Uses multi-threading to process multiple chunks in parallel.
Performs incremental aggregation after loading each chunk.

Kafka -
Kafka has been implemented to facilitate realtime communication between middleware and ETL service
Middleware sends a message to the ETL service with the CSV file name which it receives from the API
After ETL pipeline completes, the service sends a Kafka message to the middleware to send an email to the user

Heartbeat - 
A heartbeat service checks the status of the ETL service using a REST API.
The health of the ETL service is stored in-memory on the middleware. However, to scale, this can be stored in big-data for analytics.

Logs -
Logs are stored in-memory on the middleware. The ETL service sends logs to the middleware after processing each chunk, and each important milestone.
To scale, these logs can be stored in big-data.


API End Points:
Send a Kafka Message:
http://127.0.0.1:5000/process

Get Logs:
http://127.0.0.1:5000/logs

Get HeartBeat:
http://127.0.0.1:5000/heartbeat