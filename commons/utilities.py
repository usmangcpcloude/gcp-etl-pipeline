import sys
import os
import json
from datetime import date, timedelta
from google.cloud import bigquery
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, '../../'))
from configs.env_variables import *

def bigquery_run(sql_file_path, env,project,batch_id):
    """Executes a SQL file on BigQuery"""
    try:
        client = bigquery.Client()

        # Read the SQL file
        with open(sql_file_path, "r") as file:
            query = file.read()
            
        query = query.format(
        project=project,
        env="dp" if env == "prod" else "dd",  # Adjust env for dataset name
        batch_id=batch_id)    

        # Run query
        job = client.query(query)
        result = job.result()  # Waits for job to complete

        print("Query executed successfully.")
        return result
    except Exception as e:
        print(f"Failed to execute SQL file: {e}")
        sys.exit(1)