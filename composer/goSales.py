import os
from airflow import DAG
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import subprocess
from airflow.operators.bash import BashOperator


# Project and cluster configuration
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="kinetic-star-451310-s6",
    zone="us-central1-a",                # Required
    master_machine_type="n1-standard-2", # Smallest machine type for master
    worker_machine_type="n1-standard-2", # Smallest machine type for workers
    num_workers=2,                       # Number of worker nodes
    storage_bucket="dataproc-bucket-poc-project",
    master_disk_size=100,                # Smallest allowed disk size for master (in GB)
    worker_disk_size=100,                # Smallest allowed disk size for workers (in GB)
).make()

# PySpark job configuration
PYSPARK_JOB_1 = {
    "reference": {"project_id": 'kinetic-star-451310-s6'},
    "placement": {"cluster_name": 'small-cluster'},
    "pyspark_job": {"main_python_file_uri": "gs://gcp-etl-pipeline/gcp-etl-pipeline/jobs/curated/helpings/method_hlp/dl_ct_gosales_method_hlp_01.py"}
}

# PySpark job configuration
PYSPARK_JOB_2 = {
    "reference": {"project_id": 'kinetic-star-451310-s6'},
    "placement": {"cluster_name": 'small-cluster'},
    "pyspark_job": {"main_python_file_uri": "gs://gcp-etl-pipeline/gcp-etl-pipeline/jobs/curated/helpings/method_hlp/dl_ct_gosales_method_hlp_01.py"}
}


PYSPARK_JOB_3 = {
    "reference": {"project_id": 'kinetic-star-451310-s6'},
    "placement": {"cluster_name": 'small-cluster'},
    "pyspark_job": {"main_python_file_uri": "gs://gcp-etl-pipeline/gcp-etl-pipeline/jobs/curated/lookups/product_lkp/dl_ct_gosales_product_lkp_01.py"}
}

PYSPARK_JOB_4 = {
    "reference": {"project_id": 'kinetic-star-451310-s6'},
    "placement": {"cluster_name": 'small-cluster'},
    "pyspark_job": {"main_python_file_uri": "gs://gcp-etl-pipeline/gcp-etl-pipeline/jobs/curated/dimensions/retailer_dim/dl_ct_gosales_retailer_dim_01.py"}
}

PYSPARK_JOB_5 = {
    "reference": {"project_id": 'kinetic-star-451310-s6'},
    "placement": {"cluster_name": 'small-cluster'},
    "pyspark_job": {"main_python_file_uri": "gs://gcp-etl-pipeline/gcp-etl-pipeline/jobs/curated/facts/sales_fact/dl_ct_gosales_sales_fact_01.py"}
}


# Define the GCS bucket and path to your Python file
GCS_BUCKET_NAME = "gcp-etl-pipeline"
GCS_PYTHON_FILE_PATH = "gcp-etl-pipeline/jobs/semantic/dl_sm_job.py"

# Define the local path for the Python file (in the airflow worker)
LOCAL_PYTHON_FILE_PATH = "/tmp/dl_sm_job.py"

def download_from_gcs_and_run():
    # Initialize a GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_PYTHON_FILE_PATH)

    # Download the Python file to the local filesystem
    blob.download_to_filename(LOCAL_PYTHON_FILE_PATH)

    # Run the Python file using subprocess
    result = subprocess.run(['python3', LOCAL_PYTHON_FILE_PATH], capture_output=True, text=True)

    # Print the result
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")

    # Return the result code (0 means success)
    return result.returncode


def generate_batch_id():
    return '999'

# DAG definition
with DAG(
    'dataproc_small_cluster_dag',
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),  # Start date for the DAG
    catchup=False,           # Disable catchup
) as dag:

    create_batch_id = PythonOperator(
        task_id='generate_batch_id',
        python_callable=generate_batch_id
    )


    setup_environment = BashOperator(
        task_id='setup_environment',
        bash_command='''
            # Create working directory
            WORK_DIR="/home/airflow/gcs/data/gosales_pipeline"
            mkdir -p $WORK_DIR
            cd $WORK_DIR
        
        # Download required files from GCS
        for file in dl_rw_job.py setup.py db_configs.py utilities.py Job_Meta_Details.py service-account-compute-addo.json env_variables.py
        do
            gsutil cp gs://apache_beam_dataflow_bucket/$file .
            if [ $? -ne 0 ]; then
                echo "Failed to download $file"
                exit 1
            fi
        done
        
        
        # Set permissions
        chmod 644 *.py
        chmod 600 service-account-compute-addo.json
        
        echo "Environment setup complete. Contents:"
        ls -la
    '''
    )

    dl_rw_gosales_go_methods = BashOperator(
        task_id='dl_rw_gosales_go_methods',
        bash_command='''
        
            cd /home/airflow/gcs/data/gosales_pipeline
            ls -la
            python --version
        
            echo "Starting pipeline execution..."
            python dl_rw_job.py \
                gosales \
                gosales \
                go_methods \
                dev \
                {{ task_instance.xcom_pull(task_ids="generate_batch_id") }}
        '''
    )


    dl_rw_gosales_go_retailers = BashOperator(
        task_id='dl_rw_gosales_go_retailers',
        bash_command='''
        
            cd /home/airflow/gcs/data/gosales_pipeline
            ls -la
            python --version
        
            echo "Starting pipeline execution..."
            python dl_rw_job.py \
                gosales \
                gosales \
                go_retailers \
                dev \
                {{ task_instance.xcom_pull(task_ids="generate_batch_id") }}
        '''
    ) 


    dl_rw_gosales_go_products = BashOperator(
        task_id='dl_rw_gosales_go_products',
        bash_command='''
        
            cd /home/airflow/gcs/data/gosales_pipeline
            ls -la
            python --version
        
            echo "Starting pipeline execution..."
            python dl_rw_job.py \
                gosales \
                gosales \
                go_products \
                dev \
                {{ task_instance.xcom_pull(task_ids="generate_batch_id") }}
        '''
    ) 


    # Task to create the Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',  # Required
        cluster_name='small-cluster',       # Required
        region='us-central1',               # Required
        cluster_config=CLUSTER_GENERATOR_CONFIG,  # Required
    )

    # Task to submit the PySpark job
    pyspark_job = DataprocSubmitJobOperator(
        task_id="dl_ct_gosales_method_hlp_01",
        job=PYSPARK_JOB_1,
        region='us-central1',
        project_id='kinetic-star-451310-s6'
    )


        # Task to submit the PySpark job
    pyspark_job_2 = DataprocSubmitJobOperator(
        task_id="dl_ct_gosales_retailer_hlp_01",
        job=PYSPARK_JOB_2,
        region='us-central1',
        project_id='kinetic-star-451310-s6'
    )

    pyspark_job_3 = DataprocSubmitJobOperator(
        task_id="dl_ct_gosales_product_lkp_01",
        job=PYSPARK_JOB_3,
        region='us-central1',
        project_id='kinetic-star-451310-s6'
    )

    pyspark_job_4 = DataprocSubmitJobOperator(
        task_id="dl_ct_gosales_retailer_dim_01",
        job=PYSPARK_JOB_4,
        region='us-central1',
        project_id='kinetic-star-451310-s6'
    )


    pyspark_job_5 = DataprocSubmitJobOperator(
        task_id="dl_ct_gosales_sales_fact_01",
        job=PYSPARK_JOB_5,
        region='us-central1',
        project_id='kinetic-star-451310-s6'
    )



    # Task to download the Python file from GCS and run it
    tl_sm_sales_overview_o1 = PythonOperator(
        task_id='tl_sm_sales_overview_o1',
        python_callable=download_from_gcs_and_run,
    )



    # Task to delete the Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='kinetic-star-451310-s6',
        cluster_name='small-cluster',
        region='us-central1'
    )

    # Define task dependencies
    # Setup dependencies
    create_batch_id >> setup_environment
    setup_environment >> dl_rw_gosales_go_methods >> dl_rw_gosales_go_products >> dl_rw_gosales_go_retailers >> create_cluster
    #dl_rw_gosales_go_methods >> [pyspark_job, pyspark_job_2, pyspark_job_3]
    #dl_rw_gosales_go_products >> [pyspark_job, pyspark_job_2, pyspark_job_3]
    #dl_rw_gosales_go_retailers >> [pyspark_job, pyspark_job_2, pyspark_job_3]
    create_cluster >> [pyspark_job, pyspark_job_2, pyspark_job_3]
    pyspark_job_2 >> pyspark_job_4
    [pyspark_job, pyspark_job_4, pyspark_job_3] >> pyspark_job_5
    pyspark_job_5 >> [delete_cluster, tl_sm_sales_overview_o1]
