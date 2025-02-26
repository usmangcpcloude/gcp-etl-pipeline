from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "kinetic-star-451310-s6"
REGION = "us-central1"
CLUSTER_NAME = "cluster-17c8"
BUCKET_NAME = "us-central1-poc-fc56a69e-bucket"

PYSPARK_JOB_URI = f"gs://{BUCKET_NAME}/dags/gcp-etl-pipeline/jobs/curated/lookups/product_lkp/dl_ct_gosales_product_lkp_01.py"

FILES = [
    f"gs://{BUCKET_NAME}/dags/gcp-etl-pipeline/commons/utilities.py",
    f"gs://{BUCKET_NAME}/dags/gcp-etl-pipeline/commons/Job_Meta_Details.py",
    f"gs://{BUCKET_NAME}/dags/gcp-etl-pipeline/configs/db_configs.py",
    f"gs://{BUCKET_NAME}/dags/gcp-etl-pipeline/configs/env_variables.py"
]

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_JOB_URI,
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

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id="product_lkp_ct",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    submit_dataproc_job
