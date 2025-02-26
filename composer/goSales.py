from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid
import os
import sys

# Define base directories
BASE_DIR = "/home/airflow/gcs/dags/gcp-etl-pipeline"
MAIN_SCRIPT_DIRECTORY = f"{BASE_DIR}/jobs/"

# Append paths for custom modules
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_dir, '../'))
sys.path.insert(1, f"{BASE_DIR}/configs/")
sys.path.insert(2, f"{BASE_DIR}/commons/")

# Import project-specific modules
from configs.db_configs import *
from commons.utilities import *

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 23),
}

# Define DAG
dag = DAG(
    'goSales',
    default_args=default_args,
    description='Pipeline to extract GoSales data and load into BigQuery',
    schedule_interval=None,
    catchup=False
)

# Task 1: Generate Batch ID
def generate_batch_id(**kwargs):
    batch_id = '999'  # Static for now; can be dynamic
    kwargs['ti'].xcom_push(key='batch_id', value=batch_id)
    return batch_id

create_batch_id = PythonOperator(
    task_id='generate_batch_id',
    python_callable=generate_batch_id,
    provide_context=True,
    dag=dag
)

# Task 2: Run Pipeline with Dynamic Batch ID
run_pipeline = BashOperator(
    task_id='go_methods',
    bash_command=f'python {MAIN_SCRIPT_DIRECTORY}raw/dl_rw_job.py gosales gosales go_methods dev "{{{{ ti.xcom_pull(task_ids="generate_batch_id", key="batch_id") }}}}"',
    dag=dag
)

# Define task dependencies
create_batch_id >> run_pipeline
