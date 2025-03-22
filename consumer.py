import csv
from flask import Flask, jsonify
import threading
import smtplib
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import time
import mysql.connector
from sqlalchemy.pool import QueuePool
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import requests
from utils import EMAIL_TOPIC, KAFKA_BROKER, KAFKA_TOPIC, MIDDLEWARE_URL, create_producer, create_consumer, consume_messages as consume_kafka_messages

app = Flask(__name__)
consumer_status = {"alive": False}

producer = create_producer(KAFKA_BROKER)
        
# MySQL connection setup with connection pooling
engine = create_engine(
    'mysql+pymysql://root:@localhost/yelp_db',
    poolclass=QueuePool,
    pool_size=10,  
    max_overflow=20,)

status_lock = Lock()

def format_log(message):
    return message

def consume_messages():
    """Consume messages from Kafka and update status"""
    global consumer_status

    # Initialize Kafka Consumer
    consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC, "email_group")
    consumer_status["alive"] = True
    print("Kafka Consumer started... Listening for messages.")

    consume_kafka_messages(consumer, read_csv)

def read_csv(file_path):
    """Read and print contents of a CSV file"""
    try:
        with app.app_context():
            
            process_csv_in_chunks(file_path)
            producer.send(EMAIL_TOPIC, "message processed")
            producer.flush()
    except FileNotFoundError:
        print(f"File {file_path} not found.")

def send_log(log):
    try:
        res = requests.post(f"{MIDDLEWARE_URL}/logs", json={"log":log})
        if res.status_code != 200:
            print(f"Error sending log: {res}")
    except Exception as e:
        print(f"Error sending log: {e}")

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    """Heartbeat endpoint to check consumer status"""
    return jsonify({"consumer_status": "running" if consumer_status["alive"] else "stopped"})
# Function to clean data
def clean_data(df):
    try:
        
        df_cleaned = df.drop_duplicates()

        if "OLF" in df_cleaned.columns:
            df_cleaned = df_cleaned.drop(columns=["OLF"])      

        df_cleaned["Phone"] = df_cleaned["Phone"].fillna('').astype(str)
        df_cleaned["Phone"] = df_cleaned["Phone"].str.replace(r'\D', '', regex=True)

        df_cleaned["Organization"] = df_cleaned.groupby("Phone")["Organization"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown"))

        df_cleaned["City"] = df_cleaned.groupby("Phone")["City"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown"))

        df_cleaned["State"] = df_cleaned.groupby("City")["State"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown") )

        df_cleaned["Street"] = df_cleaned.groupby(["City", "Organization"])["Street"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown") )

        df_cleaned["Building"] = df_cleaned.groupby(["City", "Organization"])["Building"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown"))

        df_cleaned["Time_GMT"] = pd.to_datetime(df_cleaned["Time_GMT"], errors="coerce")

        df_cleaned["NumberReview"] = df_cleaned["NumberReview"].astype("int32")
        df_cleaned["Rating"] = df_cleaned["Rating"].astype("float32")

        return df_cleaned

    except Exception as e:
        send_log(format_log(f"Error in clean_data: {e}"))
        raise  

# Function to load data into the database
def load_data_to_db(df):
    try:
        df.to_sql('yelp_data', con=engine, if_exists='append', index=False, method='multi')
        send_log(format_log(f"Data inserted successfully, {df.shape[0]} rows."))
    except Exception as ex:
        send_log(format_log(f"Error inserting data: {ex}"))
        raise

# Function to create tables
def create_tables():
    try:
        connection = mysql.connector.connect(host='localhost', user='root', password='', database='yelp_db')
        cursor = connection.cursor()

        create_yelp_table_query = """CREATE TABLE IF NOT EXISTS yelp_data (
            ID INT PRIMARY KEY,
            Time_GMT DATETIME,
            Phone VARCHAR(15),
            Organization VARCHAR(255),
            Rating FLOAT,
            NumberReview INT,
            Category VARCHAR(50),
            Country VARCHAR(50),
            CountryCode VARCHAR(5),
            State VARCHAR(50),
            City VARCHAR(100),
            Street VARCHAR(255),
            Building VARCHAR(100),
            INDEX idx_category (Category),
            INDEX idx_state (State),
            INDEX idx_city (City),
            INDEX idx_rating (Rating)
        ) ENGINE=InnoDB;"""
        
        create_aggregated_sales_query = """CREATE TABLE IF NOT EXISTS aggregated_category_state_ratings (
            Category VARCHAR(50),
            State VARCHAR(50),
            average_rating FLOAT,
            PRIMARY KEY (Category, State)
        ) ENGINE=InnoDB;"""
        
        cursor.execute(create_yelp_table_query)
        cursor.execute(create_aggregated_sales_query)
        connection.commit()
        send_log(format_log("Tables created successfully."))

    except mysql.connector.Error as err:
        send_log(format_log(f"Database error: {err}"))
    except Exception as err:
        send_log(format_log(f"General error: {err}"))
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Function to process each chunk 
def process_chunk(chunk):
    try:
        clean_chunk = clean_data(chunk)
        load_data_to_db(clean_chunk)

        #Aggregation for the chunk
        with engine.connect() as conn:
            aggregate_query = """
                INSERT INTO aggregated_category_state_ratings (Category, State, average_rating)
                SELECT 
                Category, 
                State, 
                AVG(Rating) AS Average_Rating
                FROM 
                yelp_data
                GROUP BY 
                Category, State
                ORDER BY 
                Average_Rating DESC
                ON DUPLICATE KEY UPDATE 
                average_rating = VALUES(average_rating);
            """
            conn.execute(aggregate_query)
        
        with status_lock:
            etl_status['processed_chunks'] += 1
        send_log(format_log(f"Processed chunk with {clean_chunk.shape[0]} rows."))

    except Exception as e:
        with status_lock:
            etl_status['errors'] += 1
        send_log(format_log(f"Error processing chunk: {e}"))

def process_csv_in_chunks(csv_file, chunk_size=10000, max_workers=8):
    
    create_tables()
    
    # Read CSV in chunks and process it
    chunk_iter = pd.read_csv(csv_file, chunksize=chunk_size)

    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for chunk in chunk_iter:
                executor.submit(process_chunk, chunk)  
    except Exception as e:
        send_log(format_log(f"Error during chunk processing: {e}"))

    end_time = time.time()
    send_log(format_log(f"Total execution time: {round((end_time - start_time) / 60, 2)} minutes."))


if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    try:
        consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        consumer_thread.start()
    except Exception as e:
        print(f"Error starting consumer thread: {e}")
        consumer_status = {"alive": False}

    
    # Run Flask app on a different port
    app.run(port=5001)