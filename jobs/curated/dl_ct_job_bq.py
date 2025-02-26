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
from configs.db_configs import *
from commons.utilities import  *

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

    # Run the SQL file on BigQuery
    sql_file_path = os.path.normpath(os.path.join(script_dir_format, use_case, sql_file))
    project,mysql_etl_monitoring=env_configs(env)
    bigquery_run(sql_file_path, env,project,batch_id)        