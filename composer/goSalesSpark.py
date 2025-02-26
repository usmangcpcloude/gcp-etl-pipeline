from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "kinetic-star-451310-s6"
REGION = "us-central1"
CLUSTER_NAME = "cluster-17c8"
BASE_BUCKET_PATH = "gs://us-central1-poc-fc56a69e-bucket/dags/gcp-etl-pipeline"
JOBS_PATH=f"{BASE_BUCKET_PATH}/jobs"
COMMONS_PATH=f"{BASE_BUCKET_PATH}/commons"
CONFIGS_PATH=f"{BASE_BUCKET_PATH}/configs"
# List of different PySpark job scripts with task names
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

def create_pyspark_job(pyspark_job_uri):
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": pyspark_job_uri,
            "python_file_uris": FILES
        }
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    "start_date": days_ago(1),
}

with DAG(
    "goSalesSpark",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or set cron expression
    catchup=False,
) as dag:
        
    previous_task = None
    for task_name, pyspark_job_uri in PYSPARK_JOBS:
        task = DataprocSubmitJobOperator(
            task_id=task_name,
            job=create_pyspark_job(pyspark_job_uri),
            region=REGION,
            project_id=PROJECT_ID
        )

        if previous_task:
            previous_task >> task  # Ensuring sequential execution
        previous_task = task