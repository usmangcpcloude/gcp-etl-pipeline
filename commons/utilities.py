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
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.parquetio import WriteToParquet
import logging
import argparse
import pyarrow
import re
from google.cloud import kms
import base64

script_dir = os.path.dirname(os.path.abspath(__file__))  
script_dir_format=script_dir
jobs_dir = os.path.dirname(script_dir)
project_root = os.path.dirname(jobs_dir)
sys.path.append(project_root)
try:
    from configs.db_configs import *
    from commons.utilities import *
    from commons.Job_Meta_Details import Job_Meta_Details
    from configs.env_variables import variables
except ModuleNotFoundError:
    from db_configs import *
    from utilities import *
    from env_variables import variables
    from Job_Meta_Details import Job_Meta_Details


KMS_KEY_PATH = "projects/kinetic-star-451310-s6/locations/global/keyRings/default-keyring/cryptoKeys/default-key"


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
            region = configs[env]['region']
            mysql_etl_monitoring = configs[env]['mysql_etl_monitoring']
            return (
                project,region, mysql_etl_monitoring
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
    staging_bucket = f"gs://{prefix}{staging_bucket}"
    raw_bucket = f"gs://{prefix}{raw_bucket}"
    
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


def convert_type(value, col_type):
    if value is None:
        return None  # Handle NULL values    
    try:
        if col_type in ['tinyint', 'smallint', 'mediumint', 'int', 'bigint']:
            return int(value)
        elif col_type in ['float', 'double', 'decimal', 'numeric']:
            return float(value)
        elif col_type in ['date']:
            return str(value)  # Store dates as strings in 'YYYY-MM-DD' format
        elif col_type in ['datetime', 'timestamp']:
            return str(value)  # Store datetime as string 'YYYY-MM-DD HH:MM:SS'
        elif col_type in ['time']:
            return str(value)  # Store time as string 'HH:MM:SS'
        elif col_type in ['year']:
            return int(value)  # Store year as integer
        elif col_type in ['char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext']:
            return str(value)
        elif col_type in ['binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob']:
            return bytes(value)  # Store binary as raw bytes
        elif col_type in ['bit']:
            return int.from_bytes(value, byteorder='big')  # Convert bit field to integer
        else:
            return str(value)  # Default fallback to string
    except Exception as e:
        print(f"Error converting value {value} of type {col_type}: {e}")
        return None  # Handle conversion errors safely

def encrypt_with_kms(plaintext: str) -> str:
    """Encrypts a given string using Google Cloud KMS."""
    client = kms.KeyManagementServiceClient()
    encrypted_response = client.encrypt(request={"name": KMS_KEY_PATH, "plaintext": plaintext.encode()})
    return base64.b64encode(encrypted_response.ciphertext).decode()

def dataflow_pipeline_run(pipeline_options,table_name,env_raw_bucket,db_secret_name,project,data_types,column_names):
    host, username, password, database,ssl=get_credentials(db_secret_name,project)
    
    parquet_schema = pyarrow.schema([
    (name,
        pyarrow.int32() if col_type in ['tinyint', 'smallint', 'mediumint', 'int', 'year'] else
        pyarrow.int64() if col_type == 'bigint' else
        pyarrow.float32() if col_type == 'float' else
        pyarrow.float64() if col_type in ['double', 'decimal', 'numeric'] else
        pyarrow.string() if col_type in ['char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext'] else
        pyarrow.string() if col_type in ['date', 'datetime', 'timestamp', 'time'] else
        pyarrow.binary() if col_type in ['binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob'] else
        pyarrow.int32() if col_type == 'bit' else
        pyarrow.string()  # Default fallback to string
        )
        for name, col_type in data_types.items()
                    ])
    
    sql_query = f"""
                    SELECT 
                        {', '.join(
                            f"CAST({name} AS CHAR) AS {name}" if data_types[name] in ['date', 'timestamp'] else name
                            for name in column_names
                        )}
                    FROM {database}.{table_name}
                    """
    def row_to_dict(row):
        return {name: convert_type(row[i], data_types[name]) for i, name in enumerate(column_names)}
    
    def row_to_dict_encrypted(row):
        """Converts row to dict and encrypts selected columns."""
        encrypted_columns = ["product_brand"]  # Define columns to encrypt
        return {
            name: encrypt_with_kms(str(row[i])) if name in encrypted_columns else row[i]
            for i, name in enumerate(column_names)
        }


    with beam.Pipeline(options=pipeline_options) as pipeline:
        mysql_data = (
            pipeline 
            | 'Read from MySQL' >> ReadFromJdbc(
                driver_class_name='com.mysql.cj.jdbc.Driver',
                jdbc_url="jdbc:mysql://"+host+":3306/"+database,
                username=username,
                password=password,
                query=sql_query,
                table_name=database+'.'+table_name
            )
            | 'Convert Row to Dict' >> beam.Map(row_to_dict_encrypted)
            | 'Strip Whitespace' >> beam.Map(lambda row: {k: v.strip() if isinstance(v, str) else v for k, v in row.items()})  # Strip \r and spaces
        )
        
        # Write data to Parquet format in GCS
        mysql_data | 'Write to Parquet' >> WriteToParquet(
            file_path_prefix=env_raw_bucket+'/'+database+'/'+table_name +'/'+ table_name,
            schema=parquet_schema,
            file_name_suffix='.parquet'
        )



###################################################################################################################
#                                             SECTION: utilities.py                                    #
###################################################################################################################
#################################################################################
#
#    Name :  runMySQLQuery()
#    Purpose: this function is responsible for executing mysql query based on select type operator in a params
#    ----------------------------------------------------------------------------
#
#    Parameters :  connection, query, data, statement_type
#                 
#    ----------------------------------------------------------------------------
#
#    Return Type : None
#
#################################################################################
def runMySQLQuery(connection, query, data, statement_type='SELECT'):
    cursor = connection.cursor()
    # Execute the appropriate SQL statement based on the statement_type parameter
    if statement_type == 'SELECT':
        cursor.execute(query, data) 
        result = cursor.fetchall()
        cursor.close()
        return result
    elif statement_type == 'INSERT':
        cursor.execute(query, data)
        connection.commit()
        cursor.close()
    elif statement_type == 'UPDATE':
        cursor.execute(query, data)
        connection.commit()
        cursor.close()
    else:
        raise ValueError('Invalid statement type specified')
    
###################################################################################################################
#                                             SECTION: utilities.py                                    #
###################################################################################################################
#################################################################################
#
#    Name :  set_etl_log_details()
#    Purpose: this function is responsible for inserting log details related to an ETL jobs execution into a MySQL database.
#    ----------------------------------------------------------------------------
#
#    Parameters :  Job_Meta_Details, MySQLConnection)
#                 
#    ----------------------------------------------------------------------------
#
#    Return Type : None
#
#################################################################################
def set_etl_log_details(Job_Meta_Details, MySQLConnection):
  
    query_etl_log_details = (f"INSERT INTO {monitoring_db}.{operational} "
                             "(batch_id, table_id, db_name, table_schema, table_name, layer, rows_ingested, job_start_time, job_end_time, job_duration, job_status, `exception`, remarks, src_extraction_type, raw_ingestion_type,job_name)"
                             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

    data_etl_log_details = [Job_Meta_Details.BATCH_ID, Job_Meta_Details.TABLE_ID, Job_Meta_Details.DATABASE, Job_Meta_Details.SCHEMA_NAME, Job_Meta_Details.TABLE_NAME.lower(), Job_Meta_Details.LAYER, Job_Meta_Details.ROWS_INGESTED, Job_Meta_Details.JOB_START_TIME, Job_Meta_Details.JOB_END_TIME, Job_Meta_Details.JOB_EXECUTION_TIME, Job_Meta_Details.JOB_STATUS, Job_Meta_Details.EXCEPTION, Job_Meta_Details.REMARKS, Job_Meta_Details.SRC_EXTRACTION_TYPE, Job_Meta_Details.RAW_INGESTION_TYPE,Job_Meta_Details.JOB_NAME]
    runMySQLQuery(MySQLConnection,query_etl_log_details, data_etl_log_details, statement_type='INSERT')



###################################################################################################################
#                                             SECTION: utilities.py                                    #
###################################################################################################################
#################################################################################
#
#    Name :  upsert_meta_info()
#    Purpose:  SQL query to upsert operational metadata into a MySQL table. 
#    ----------------------------------------------------------------------------
#
#    Parameters :  Job_Meta_Details, MySQLConnection
#                 
#    ----------------------------------------------------------------------------
#
#    Return Type : None
#
#################################################################################
def upsert_meta_info(Job_Meta_Details, MySQLConnection):
    #end_time.isoformat(' ')
    Job_Meta_Details.JOB_END_TIME = datetime.now()  #removed ".replace(microsecond=0)" because it was causing troubles in job execution time calculation
    print(Job_Meta_Details.JOB_START_TIME)
    #job_start_time = datetime.strptime(Job_Meta_Details.JOB_START_TIME, '%Y-%m-%d %H:%M:%S.%f'))
    JOB_EXECUTION_TIME=str(Job_Meta_Details.JOB_END_TIME - Job_Meta_Details.JOB_START_TIME)
    Job_Meta_Details.JOB_EXECUTION_TIME = JOB_EXECUTION_TIME
    #formatted_start_time = Job_Meta_Details.JOB_START_TIME.isoformat(sep=' ')
    set_etl_log_details(Job_Meta_Details, MySQLConnection)



###################################################################################################################
#                                             SECTION: utilities.py                                    #
###################################################################################################################
#################################################################################
#
#    Name :   record_exception()
#    Purpose: this function will return record exception
#    ----------------------------------------------------------------------------
#
#    Parameters :  sql connection, message, Job_Meta_Details
#                 
#    ----------------------------------------------------------------------------
#
#    Return Type : None
#
#################################################################################
def record_exception(Job_Meta_Details, e, message,MySQLConnection):
    Job_Meta_Details.JOB_STATUS = 'FAILURE'
    Job_Meta_Details.REMARKS = message
    e = re.sub(r'[^\x00-\x7F]+', ' ', str(e))
    e = e.replace('`', '').replace("'", '').replace('"', '')
    if len(str(e)) > 500:
        Job_Meta_Details.EXCEPTION = str(e)[:1500]  #increased the range of captured exception
    else:
        Job_Meta_Details.EXCEPTION = str(e)
    # conn= openMySQLConnection(env)
    upsert_meta_info(Job_Meta_Details,MySQLConnection)
    print(str(e))
    exit(1)