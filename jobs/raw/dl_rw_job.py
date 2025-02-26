from google.cloud import secretmanager
import json
import sys
import os
from datetime import datetime,timedelta
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.parquetio import WriteToParquet
import logging
import argparse
import pyarrow
script_dir = os.path.dirname(os.path.abspath(__file__))  
script_dir_format=script_dir
jobs_dir = os.path.dirname(script_dir)
project_root = os.path.dirname(jobs_dir)
sys.path.append(project_root)
from configs.db_configs import *
from commons.utilities import  *
from commons.Job_Meta_Details import Job_Meta_Details
from configs.env_variables import variables


# try:
#     BASE_DIR = "/home/airflow/gcs/dags/gcp-etl-pipeline"
#     service_account_json = os.path.join(BASE_DIR, "commons/service-account-compute-addo.json")
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json
#     setup_file_path = "/home/airflow/gcs/dags/gcp-etl-pipeline/jobs/raw/setup.py"
# except ModuleNotFoundError:
service_account_json = "commons/service-account-compute-addo.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json
setup_file_path = "jobs/raw/setup.py"



runner = variables['runner']
temp_location = variables['temp_bucket']

project = variables['project']
keyring = variables['keyring']
cryptokey = variables['cryptokey']

KMS_KEY_PATH = f"projects/{project}/locations/global/keyRings/{keyring}/cryptoKeys/{cryptokey}"


def encrypt_with_kms(plaintext: str) -> str:
    """Encrypts a given string using Google Cloud KMS."""
    client = kms.KeyManagementServiceClient()
    encrypted_response = client.encrypt(request={"name": KMS_KEY_PATH, "plaintext": plaintext.encode()})
    return base64.b64encode(encrypted_response.ciphertext).decode()

def dataflow_pipeline_run(pipeline_options,table_name,env_raw_bucket,db_secret_name,project,data_types,column_names,encypt_column):
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
        encrypted_columns = encypt_column  # Define columns to encrypt
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




if __name__ == "__main__":
    """
    This block takes important command line arguments and parses them to be used in job functions later on
    Return Type : None
    """ 
    try: 
        if len(sys.argv) < 6:
            raise ValueError("Usage: python script.py <mysql_source_system> <db_name> <table_schema> <table_name> <env> <batch_id>")        
        print("Getting User Arguments", flush =True)
        
        source_system = sys.argv[1]  
        db_name = sys.argv[2]  
        table_name = sys.argv[3]  
        env = sys.argv[4]  
        batch_id = sys.argv[5]      
        current_date = datetime.now().date()    
        if len(sys.argv) > 6:
            start_date = datetime.strptime(sys.argv[6], "%Y-%m-%d").date()
        else:
            start_date = ""        
        if len(sys.argv) > 7:
            end_date = datetime.strptime(sys.argv[7], "%Y-%m-%d").date()
        else:
            end_date = ""

        # Open a MySQL database connection
        project,region,mysql_etl_monitoring=env_configs(env)
        print("Trying to Open MYSQL connection",flush =True)
        MySQLConnection=openMySQLConnection(mysql_etl_monitoring,project)
        print("MySQLConnection connection successful", flush =True)

                # Normalize inputs to lowercase
        table_name = table_name.lower()
        db_name = db_name.lower()
        source_system = source_system.lower()
        env = env.lower()
        # Create a Job_Meta_Details object to store metadata
        print("Creating Job_Meta_Details", flush =True)
        Job_Meta_Details = Job_Meta_Details(batch_id, '-1', db_name, None, table_name, "RAW", -1, datetime.now(), None, None, "Failure", None, None,None,None,'dl_rw_job')
        print("Job_Meta_Details created", flush =True)
    except Exception as e:
        print("Usage: python script.py <mysql_source_system> <db_name> <table_schema> <table_name> <env> <batch_id>")  
        record_exception(Job_Meta_Details, e, "Failed during Getting Arguements From User.",MySQLConnection)
    try: 
        # print ("Getting Ingestion_metadata arguments")
        print("Getting Ingestion_metadata arguments", flush=True)
        ingestionParams = selectIngestionParams(MySQLConnection,db_name, source_system, table_name)

        department = ingestionParams[0]
        source_system = ingestionParams[1]
        table_schema = ingestionParams[2]
        src_extraction_type = ingestionParams[3]
        raw_ingestion_type = ingestionParams[4]
        ingestion_query = ingestionParams[5]
        watermark_col_name_1 = ingestionParams[6]
        latest_watermark_val_1 = ingestionParams[7]
        table_id = ingestionParams[8]
        is_productionised = ingestionParams[9]
        db_type = ingestionParams[10]
        table_definations = ingestionParams[11]
        secret_name = ingestionParams[12]
        staging_bucket = ingestionParams[13]
        raw_bucket = ingestionParams[14]
        print("Retrieved ingestion parameters successfully", flush=True)

        #updating job meta details with specific variables to be updated in operational metadata table
        Job_Meta_Details.SRC_EXTRACTION_TYPE=src_extraction_type
        Job_Meta_Details.RAW_INGESTION_TYPE=raw_ingestion_type  
        Job_Meta_Details.TABLE_ID=table_id  

        env_results = add_env_prefix(env, secret_name, staging_bucket, raw_bucket)

        # GETTING SECRE, STAGING BUCKET AND RAW BUCKET

        db_secret_name=env_results[0]
        env_staging_bucket=env_results[1]
        env_raw_bucket=env_results[2]  

        # GETTING COLUMN NAMES,MERGE COLUMNS, DATA TYPE , SQL QUERY AND HEADER

        column_names, merge_column, data_types,sql_query,header,encypt_column = parse_table_defination(table_definations,table_name)

    except Exception as e:
        print("Exception occurred during ingestion parameter retrieval")
        record_exception(Job_Meta_Details, e, "Failed during data selectIngestionParams.",MySQLConnection)

    try:
        print("Define pipeline options", flush=True)
        pipeline_options = PipelineOptions(
                                    runner=runner,
                                    project=project,
                                    region=region,
                                    temp_location=temp_location,
                                    job_name=source_system.lower().replace("_", "-")+'-'+db_name.lower().replace("_", "-")+'-'+table_name.lower().replace("_", "-")+'-'+raw_bucket,
                                    setup_file=setup_file_path

                                )
    except Exception as e:
        print("Exception occurred during defining pipeline options")
        record_exception(Job_Meta_Details, e, "Failed during Setting DataFlow Pipeline Options.",MySQLConnection)
    try:
        print("Running Data Flow Pipeline", flush=True)
        dataflow_pipeline_run(pipeline_options,table_name,env_raw_bucket,db_secret_name,project,data_types,column_names,encypt_column)
        Job_Meta_Details.JOB_STATUS = "SUCCESS"
            # Upsert (update or insert) job metadata information
        upsert_meta_info(Job_Meta_Details, MySQLConnection)
    except Exception as e:
        print("Exception occurred during Running Dataflow Pipeline")
        record_exception(Job_Meta_Details, e, "Failed during Ingestion Data Using Dataflow.",MySQLConnection)










