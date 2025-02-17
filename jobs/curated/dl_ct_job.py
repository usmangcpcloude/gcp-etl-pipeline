import sys
import os
import json
from datetime import date, timedelta
from google.cloud import bigquery
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, '../../'))
from configs.env_variables import *
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
    project = variables['project']
    bigquery_run(sql_file, env,project,batch_id)        