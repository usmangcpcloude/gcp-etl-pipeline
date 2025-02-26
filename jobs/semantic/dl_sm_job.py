import sys
import os
import json
from datetime import date, timedelta
from google.cloud import bigquery
script_dir = os.path.dirname(os.path.abspath(__file__))  
script_dir_format=script_dir
jobs_dir = os.path.dirname(script_dir)
project_root = os.path.dirname(jobs_dir)
sys.path.append(project_root)
#sys.path.append(jobs_dir)
print(project_root)
print(jobs_dir)
print(script_dir_format)
from configs.db_configs import *
from commons.utilities import  *
from commons.Job_Meta_Details import Job_Meta_Details
from configs.env_variables import variables


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python script.py <sql_file> <table_name> <use_case> <env> <batch_id> [<start_date>] [<end_date>]")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    table_name = sys.argv[2]
    use_case = sys.argv[3]
    env = sys.argv[4]
    batch_id = sys.argv[5]
    start_date = sys.argv[6] if len(sys.argv) > 6 else str(date.today() - timedelta(days=1))
    end_date = sys.argv[7] if len(sys.argv) > 7 else str(date.today())
    sql_file_name = os.path.basename(sql_file)
    # Remove the file extension to get the job name
    job_name = os.path.splitext(sql_file_name)[0]
    project,region,mysql_etl_monitoring=env_configs(env)
    print("Trying to Open MYSQL connection",flush =True)
    MySQLConnection=openMySQLConnection(mysql_etl_monitoring,project)
    print("MySQLConnection connection successful", flush =True)
    print(project_root)
    print(jobs_dir)
    print(script_dir_format)
    Job_Meta_Details = Job_Meta_Details(batch_id, '-1', None, None, table_name, "SEMANTIC", -1, datetime.now(), None, None, "FAILURE", None, None,None,None, job_name)

    try:
        sql_file_path = os.path.normpath(os.path.join(jobs_dir, 'jobs', 'semantic'))
        sql_file_path = os.path.normpath(os.path.join(sql_file_path, use_case, sql_file))
        bigquery_run(sql_file_path, env,project,batch_id)        
        Job_Meta_Details.JOB_STATUS = "SUCCESS"
        upsert_meta_info(Job_Meta_Details, MySQLConnection)
    except Exception as e:
        print(e)
        record_exception(Job_Meta_Details, e, "Failed during executing sql file.",MySQLConnection) 