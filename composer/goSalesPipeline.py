from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Base Directories
BASE_DIR = "/home/airflow/gcs/dags/gcp-etl-pipeline/"
BASE_BUCKET_PATH = "gs://us-central1-poc-fc56a69e-bucket/dags/gcp-etl-pipeline"
JOBS_PATH = f"{BASE_BUCKET_PATH}/jobs"
COMMONS_PATH = f"{BASE_BUCKET_PATH}/commons"
CONFIGS_PATH = f"{BASE_BUCKET_PATH}/configs"

# GCP Configs
PROJECT_ID = "kinetic-star-451310-s6"
REGION = "us-central1"
CLUSTER_NAME = "cluster-17c8"

# PySpark Jobs
PYSPARK_JOBS = [
    ("method_hlp", f"{JOBS_PATH}/curated/helpings/method_hlp/dl_ct_gosales_method_hlp_01.py"),    
    ("retailer_hlp", f"{JOBS_PATH}/curated/helpings/retailer_hlp/dl_ct_gosales_retailer_hlp_01.py"),    
    ("retailer_dim", f"{JOBS_PATH}/curated/dimensions/retailer_dim/dl_ct_gosales_retailer_dim_01.py"),
    ("product_lkp", f"{JOBS_PATH}/curated/lookups/product_lkp/dl_ct_gosales_product_lkp_01.py"),
    ("sales_fact", f"{JOBS_PATH}/curated/facts/sales_fact/dl_ct_gosales_sales_fact_01.py")
]

# Common files needed for PySpark jobs
FILES = [
    f"{COMMONS_PATH}/utilities.py",
    f"{COMMONS_PATH}/Job_Meta_Details.py",
    f"{CONFIGS_PATH}/db_configs.py",
    f"{CONFIGS_PATH}/env_variables.py"
]

# Default DAG Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(1),
}

# Define DAG
dag = DAG(
    "goSalesUnifiedDag",
    default_args=default_args,
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

# Raw Extraction Jobs
raw_tasks = []
raw_sources = ["go_methods", "go_products", "go_retailers", "go_daily_sales"]

for source in raw_sources:
    task = BashOperator(
        task_id=f"{source}_raw",
        bash_command=f"""
            cd {BASE_DIR} && \
            python jobs/raw/dl_rw_job.py gosales gosales {source} dev "{{{{ ti.xcom_pull(task_ids='start_batch', key='batch_id') }}}}"
        """,
        dag=dag 
    )
    raw_tasks.append(task)

# PySpark Transformation Jobs
def create_pyspark_job(pyspark_job_uri):
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": pyspark_job_uri,
            "python_file_uris": FILES
        }
    }

previous_task = None
pyspark_tasks = []

for task_name, pyspark_job_uri in PYSPARK_JOBS:
    task = DataprocSubmitJobOperator(
        task_id=task_name,
        job=create_pyspark_job(pyspark_job_uri),
        region=REGION,
        project_id=PROJECT_ID,
        dag=dag  
    )
    
    if previous_task:
        previous_task >> task  # Ensuring sequential execution
    previous_task = task
    pyspark_tasks.append(task)

tl_sm_sales_overview = BashOperator(
    task_id='tl_sm_gosales_overview',
    bash_command=f"""
        cd {BASE_DIR} && \
        python jobs/semantic/dl_sm_job.py tl_sm_gosales_overview_01.sql tl_sales_overview gosales_thin_layer dev "{{{{ ti.xcom_pull(task_ids='start_batch', key='batch_id') }}}}"
    """,
    dag=dag
)

# Define DAG Dependencies
create_batch_id >> raw_tasks >> pyspark_tasks >> tl_sm_sales_overview
