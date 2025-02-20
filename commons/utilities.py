from google.cloud import secretmanager
import json
import sys
import os
import json
from datetime import datetime,timedelta
import mysql.connector
import ast
from google.cloud import bigquery
from typing import NamedTuple
script_dir = os.path.dirname(os.path.abspath(__file__))  
script_dir_format=script_dir
jobs_dir = os.path.dirname(script_dir)
project_root = os.path.dirname(jobs_dir)
sys.path.append(project_root)
from configs.db_configs import *
from configs.env_variables import variables



monitoring_db = variables['monitoring_db']
batch = variables['batch']
pipeline = variables['pipeline']
task_meta = variables['task']
ingestion = variables['ingestion']
operational = variables['operational']
catalog = variables['catalog']

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

def env_configs(env):
    try:
        if env =="dev" or env == "prod":
            project = configs[env]['project']
            mysql_etl_monitoring = configs[env]['mysql_etl_monitoring']
            return (
                project, mysql_etl_monitoring
            )
        else:
            print(f"Environment '{env}' not found in configuration.")
            return None
    except KeyError as e:
        print(f"KeyError: {e}. The specified key does not exist in the configuration.")
        return None
    except Exception as ex:
        print(f"An error occurred: {ex}")
        return None


def access_secret_version(secret_name, project):
    client = secretmanager.SecretManagerServiceClient()
    secret_id=secret_name
    project_id=project
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    response = client.access_secret_version(name=name)
    secret_data = response.payload.data.decode("UTF-8")

    return json.loads(secret_data)


def get_credentials(secret_name,project):
    try:
            connection_details=access_secret_version(secret_name,project)
            host = connection_details['host']
            username = connection_details['username']
            password = connection_details['password']
            database = connection_details['database']
            ssl = connection_details['ssl']['enabled']
            return (
                host, username, password, database,ssl
            )
    except KeyError as e:
        print(f"KeyError: {e}. The specified key does not exist in the configuration.")
        return None
    except Exception as ex:
        print(f"An error occurred: {ex}")
        return None    
    
def openMySQLConnection(mysql_etl_monitoring,project):
    host, username, password, database,ssl = get_credentials(mysql_etl_monitoring,project)
    HOST = host
    USER = username
    PASSWORD = password
    DATABASE = database
    SSL=ssl
    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            # print("Connected to MySQL server")
            return connection        
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")  

def selectIngestionParams(connection,db_name, source_system, table_name):
    cursor = connection.cursor()        
    query = f"select  b.department,b.source_system ,b.table_schema, b.src_extraction_type, b.raw_ingestion_type, b.ingestion_query, b.watermark_col_name_1,b.latest_watermark_val_1, b.table_id, b.is_productionised,b.db_type ,b.table_definations,a.secret_name ,a.staging_bucket ,a.raw_bucket  from   {monitoring_db}.{ingestion} b  left join  {monitoring_db}.{catalog} a on a.catalog_id =b.catalog_id  where lower(b.db_name)='{db_name}' and lower(b.source_system) = '{source_system}' and lower(b.table_name) = '{table_name}'"
    cursor.execute(query)
    result = cursor.fetchall()
    ingestionParams = None
    for row in result:
        ingestionParams=  row
        break    
    cursor.close()
    return ingestionParams

def add_env_prefix(env, secret_name, staging_bucket, raw_bucket):
    if env not in ["dev", "prod"]:
        raise ValueError("Environment must be either 'dev' or 'prod'")
    
    prefix = "dd_" if env == "dev" else "dp_"
    
    secret_name = f"{prefix}{secret_name}"
    staging_bucket = f"{prefix}{staging_bucket}"
    raw_bucket = f"{prefix}{raw_bucket}"
    
    return secret_name, staging_bucket, raw_bucket
    
def parse_table_defination(table_definations,table_name):
    try:
        table_def = ast.literal_eval(table_definations)
        if not isinstance(table_def, dict):
            raise ValueError("Invalid table definitions format. It should be a dictionary.")
        table_def_lower = {key.lower(): value for key, value in table_def.items()}
        table_info = table_def_lower.get(table_name, {})
        column_names = table_info['Column_names']
        sql_query = f"SELECT\n    " + ",\n    ".join([f"{col}::text AS {col}" for col in column_names]) + f"\nFROM {table_name};"
        merge_column = table_info['merge_column']
        data_types = table_info['data_types']
        header = ",".join(column_names)
        return column_names, merge_column, data_types,sql_query,header
    except Exception as e:
        return f"Error generating query: {str(e)}"
    
sql_to_python = {
    'bigint': int,
    'int': int,
    'integer': int,
    'smallint': int,
    'tinyint': int,
    'decimal': float,
    'numeric': float,
    'real': float,
    'double precision': float,
    'float': float,
    'char': str,
    'varchar': str,
    'varchar(255)': str,
    'text': str,
    'boolean': bool,
    'date': str,
    'datetime': str,
    'timestamp': str,
    'time': str,
    'json': dict,
    'jsonb': dict,
    'blob': bytes,
    'bytea': bytes
}    


def create_named_tuple(column_names,data_types):
    try:        
        fields = [(col, sql_to_python[data_types[col]]) for col in column_names]
        print(fields)
        # Dynamically create the NamedTuple class
        ExampleRow = NamedTuple('ExampleRow', fields)
        return ExampleRow
    except Exception as e:
        return f"Error creating NamedTuple: {str(e)}"