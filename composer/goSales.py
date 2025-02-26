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
    'goSales',
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

go_methods = BashOperator(
    task_id='go_methods_raw',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/raw/dl_rw_job.py gosales gosales go_methods dev 999
    """,
    dag=dag
)

go_products = BashOperator(
    task_id='go_products_raw',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/raw/dl_rw_job.py gosales gosales go_products dev "{{{{ ti.xcom_pull(task_ids='generate_batch_id', key='batch_id') }}}}"
    """,
    dag=dag
)

go_retailers = BashOperator(
    task_id='go_retailers_raw',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/raw/dl_rw_job.py gosales gosales go_retailers dev "{{{{ ti.xcom_pull(task_ids='generate_batch_id', key='batch_id') }}}}"
    """,
    dag=dag
)

go_daily_sales = BashOperator(
    task_id='go_daily_sales_raw',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/raw/dl_rw_job.py gosales gosales go_daily_sales dev "{{{{ ti.xcom_pull(task_ids='generate_batch_id', key='batch_id') }}}}"
    """,
    dag=dag
)
# Define task dependencies
create_batch_id >> go_methods >> go_products >> go_retailers >> go_daily_sales





















