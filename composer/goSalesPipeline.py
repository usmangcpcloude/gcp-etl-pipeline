from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Define base directories
BASE_DIR = "/home/airflow/gcs/dags/gcp-etl-pipeline/"

# GCP Dataproc configuration
PROJECT_ID = "kinetic-star-451310-s6"
REGION = "us-central1"
CLUSTER_NAME = "cluster-17c8"
BASE_BUCKET_PATH = "gs://us-central1-poc-fc56a69e-bucket/dags/gcp-etl-pipeline"
JOBS_PATH = f"{BASE_BUCKET_PATH}/jobs"
COMMONS_PATH = f"{BASE_BUCKET_PATH}/commons"
CONFIGS_PATH = f"{BASE_BUCKET_PATH}/configs"

# Define raw job names
RAW_JOB_NAMES = ["go_methods", "go_products", "go_retailers", "go_daily_sales"]

# PySpark Jobs
PYSPARK_JOBS = [
    ("method_hlp", f"{JOBS_PATH}/curated/helpings/method_hlp/dl_ct_gosales_method_hlp_01.py"),    
    ("retailer_hlp", f"{JOBS_PATH}/curated/helpings/retailer_hlp/dl_ct_gosales_retailer_hlp_01.py"),    
    ("retailer_dim", f"{JOBS_PATH}/curated/dimensions/retailer_dim/dl_ct_gosales_retailer_dim_01.py"),
    ("product_lkp", f"{JOBS_PATH}/curated/lookups/product_lkp/dl_ct_gosales_product_lkp_01.py"),
    ("sales_fact", f"{JOBS_PATH}/curated/facts/sales_fact/dl_ct_gosales_sales_fact_01.py")
]

FILES = [
    f"{COMMONS_PATH}/utilities.py",
    f"{COMMONS_PATH}/Job_Meta_Details.py",
    f"{CONFIGS_PATH}/db_configs.py",
    f"{CONFIGS_PATH}/env_variables.py"
]

# Function to create a Dataproc PySpark job
def create_pyspark_job(pyspark_job_uri):
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": pyspark_job_uri,
            "python_file_uris": FILES
        }
    }

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(1),
}

# Define DAG
with DAG(
    "goSalesPipeline",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or set cron expression
    catchup=False,
) as dag:

    # Task 1: Generate Batch ID
    def generate_batch_id(**kwargs):
        batch_id = 999  # Static for now; can be dynamic
        kwargs['ti'].xcom_push(key='batch_id', value=batch_id)
        return batch_id

    create_batch_id = PythonOperator(
        task_id='start_batch',
        python_callable=generate_batch_id,
        provide_context=True
    )

    # Dynamically create Raw Jobs
    raw_tasks = []
    for raw_job in RAW_JOB_NAMES:
        task = BashOperator(
            task_id=f"{raw_job}_raw",
            bash_command=f"""
                cd {BASE_DIR} && \
                python jobs/raw/dl_rw_job.py gosales gosales {raw_job} dev "{{{{ ti.xcom_pull(task_ids='start_batch', key='batch_id') }}}}"
            """
        )
        raw_tasks.append(task)

    # Chain raw jobs in sequence
    for i in range(len(raw_tasks) - 1):
        raw_tasks[i] >> raw_tasks[i + 1]

    # Dynamically create PySpark jobs
    pyspark_tasks = []
    for task_name, pyspark_job_uri in PYSPARK_JOBS:
        pyspark_task = DataprocSubmitJobOperator(
            task_id=task_name,
            job=create_pyspark_job(pyspark_job_uri),
            region=REGION,
            project_id=PROJECT_ID
        )
        pyspark_tasks.append(pyspark_task)

    # Chain PySpark jobs in sequence
    for i in range(len(pyspark_tasks) - 1):
        pyspark_tasks[i] >> pyspark_tasks[i + 1]

    # Final Thin Layer job
    tl_sm_sales_overview = BashOperator(
        task_id='tl_sm_gosales_overview',
        bash_command=f"""
            cd {BASE_DIR} && \
            python jobs/semantic/dl_sm_job.py tl_sm_gosales_overview_01.sql tl_sales_overview gosales_thin_layer dev "{{{{ ti.xcom_pull(task_ids='start_batch', key='batch_id') }}}}"
        """
    )

    # Define DAG dependencies
    create_batch_id >> raw_tasks[0]  # Start batch → First raw job
    raw_tasks[-1] >> pyspark_tasks[0]  # Last raw job → First PySpark job
    pyspark_tasks[-1] >> tl_sm_sales_overview  # Last PySpark job → Thin Layer job
