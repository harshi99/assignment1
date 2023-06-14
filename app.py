from flask import Flask, render_template, request
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os
import csv
import pyodbc
from datetime import datetime

app = Flask(__name__)

# Blob Storage configuration
blob_connection_string = 'DefaultEndpointsProtocol=https;AccountName=assdata1;AccountKey=WMGVFc5Btn/cWP1ErRdsoFKp+VOWcfM9r5C6uOYSod9jeunIxoThQp+A6ecG6R48CFywsaCRl/AZ+ASttwd/CA==;EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
container_name = 'assdata1-1'

# SQL configuration
server = os.environ.get('server')
database = os.environ.get('database)
username = os.environ.get('username')
password = os.environ.get('password')
driver = os.environ.get('driver')

# Function to execute SQL query
def execute_sql_query(query):
    with pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}') as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
    return columns, rows

# Route for home page
@app.route('/')
def home():
    return render_template('index.html')

# Route for file upload
@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    filename, ext = os.path.splitext(file.filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    blob_name = f'{filename}_{timestamp}{ext}'
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    with open(os.path.join('uploads', blob_name), "wb") as data:
        file.save(data)

    with open(os.path.join('uploads', blob_name), "rb") as data:
        blob_client.upload_blob(data)

    os.remove(os.path.join('uploads', blob_name))

    return 'File uploaded successfully'


# Route for SQL query execution
@app.route('/sql', methods=['POST'])
def execute_query():
    query = request.form['query']
    columns, rows = execute_sql_query(query)
    
    if len(rows) == 0:
        return 'No results found'
    
    result = []
    for row in rows:
        data = dict(zip(columns, row))
        if 'picture' in data:
            picture_url = data['picture']
            if picture_url:
                container = blob_service_client.get_container_client(container_name)
                blob_client = container.get_blob_client(picture_url)
                data['picture'] = blob_client.url
        result.append(data)
    return render_template('query_result.html', result=result)

if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)  # Create the "uploads" directory if it doesn't exist
    port = os.environ.get('PORT', 5000)
    app.run(debug=True, port=port)
