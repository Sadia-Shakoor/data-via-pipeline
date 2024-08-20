import base64
import json
import csv
import os
from google.cloud import storage
import pymysql
from google.cloud.sql.connector import Connector


def read_csv_from_gcs(bucket_name, file_name):
    """Reads a CSV file from GCS and returns the data as a list of dictionaries."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text().splitlines()
    reader = csv.DictReader(content)
    return list(reader)

def read_json_from_gcs(bucket_name, file_name):
    """Reads a JSON file from GCS and returns the data."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return json.loads(blob.download_as_text())

def read_cloud_sql_table(host, user, password, database, query):
    """Reads data from a Cloud SQL table and returns the result."""
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return result

# def read_cloud_sql_table(host, user, password, database, query):
# def read_cloud_sql_table(host, user, password, database, query):
# def read_cloud_sql_table(user, password, database, query):
#     # Initialize Connector object
#     connector = Connector()

#     # Function to configure the connection
#     def getconn():
#         conn = connector.connect(
#             os.environ['CLOUD_SQL_HOST'],
#             "pymysql",
#             user=user,
#             password=password,
#             db=database
#         )
#         return conn

#     connection = getconn()
#     cursor = connection.cursor(pymysql.cursors.DictCursor)
#     cursor.execute(query)
#     result = cursor.fetchall()
#     connection.close()
#     return result

def write_json_to_gcs(bucket_name, file_name, data):
    """Writes JSON data to a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')

def main_etl():
    """Main ETL process to read, process, and write data."""
    # Extract data from environment variables
    csv_file_path = os.getenv('CSV_FILE_PATH')
    json_file_path = os.getenv('JSON_FILE_PATH')
    cloud_sql_query = 'select * from employee;'
    cloud_sql_details = {
        'host': os.getenv('CLOUD_SQL_HOST'),
        'user': os.getenv('CLOUD_SQL_USER'),
        'password': os.getenv('CLOUD_SQL_PASSWORD'),
        'database': os.getenv('CLOUD_SQL_DATABASE')
    }
    output_json_path = 'combined_data.json'
    bucket_name = os.getenv('BUCKET_NAME')

    # Read data
    csv_data = read_csv_from_gcs(bucket_name, csv_file_path)
    json_data = read_json_from_gcs(bucket_name, json_file_path)
    cloud_sql_data = read_cloud_sql_table(cloud_sql_details['host'], cloud_sql_details['user'], cloud_sql_details['password'], cloud_sql_details['database'], cloud_sql_query)

    # Combine data
    combined_data = {
        'csv_data': csv_data,
        'json_data': json_data,
        'cloud_sql_data': cloud_sql_data
    }

    # Write combined data to JSON in Cloud Storage
    write_json_to_gcs(bucket_name, output_json_path, combined_data)

    print('ETL process completed successfully')

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic."""
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Received message: {pubsub_message}")
    
    # Call the ETL process
    main_etl()
