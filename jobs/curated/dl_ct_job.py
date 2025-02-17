import mysql.connector
import shlex
import sys
import subprocess
import ast
import json
from datetime import datetime,timedelta,date
import re
import os
import tempfile

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, '../../'))
from configs.env_variables import *
from configs.db_configs import *
from commons.Job_Meta_Details import Job_Meta_Details
from commons.utilities import  *
import subprocess
if __name__ == "__main__":
    # Check for the correct number of command-line arguments
    if len(sys.argv) < 5:
        print("Usage: python script.py <sql_file> <start_date> <end_date> <execution_engine>")
        sys.exit(1)    
    sql_file = sys.argv[1]
    table_name=sys.argv[2]
    use_case=sys.argv[3]
    env = sys.argv[4]
    batch_id = sys.argv[5]
    start_date = sys.argv[6] if len(sys.argv) > 6 else str(date.today() - timedelta(days=1))
    end_date = sys.argv[7] if len(sys.argv) > 7 else str(date.today())
    sql_file_name = os.path.basename(sql_file)
    job_name = os.path.splitext(sql_file_name)[0]
    

def bigquery_run(query ,env):
    "write code to run given file on Big Query"

try:
    result = execute_trino_query_clickhouse(sql_file_path, env)    
except Exception as e:
    print("Failed during executing sql file.")
        
    