from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid
import os
import sys

# Define base directories
BASE_DIR = "/home/airflow/gcs/dags/gcp-etl-pipeline/"

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2024, 2, 23),
}

# Define DAG
dag = DAG(
    'thinLayer',
    default_args=default_args,
    description='Pipeline to extract GoSales data and load into BigQuery',
    schedule_interval=None,
    catchup=False
)

# Task 1: Generate Batch ID
def generate_batch_id(**kwargs):
    batch_id = 999  # Static for now; can be dynamic
    kwargs['ti'].xcom_push(key='batch_id', value=batch_id)
    return batch_id

create_batch_id = PythonOperator(
    task_id='start_batch',
    python_callable=generate_batch_id,
    provide_context=True,
    dag=dag
)

tl_sm_sales_overview = BashOperator(
    task_id='tl_sm_gosales_overview',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/semantic/dl_sm_job.py tl_sm_gosales_overview_01.sql tl_sales_overview gosales_thin_layer dev "{{{{ ti.xcom_pull(task_ids='start_batch', key='batch_id') }}}}"
    """,
    dag=dag
)


# Define task dependencies
create_batch_id >> tl_sm_sales_overview 





















