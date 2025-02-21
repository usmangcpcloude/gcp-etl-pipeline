import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.parquetio import WriteToParquet
import logging
import argparse
import pyarrow

# Set Google Cloud credentials
service_account_json = "commons/service-account-compute-addo.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json

project = "kinetic-star-451310-s6"
jdbc_url = "jdbc:mysql://34.60.103.140:3306/gosales"
username = "usmanzafar@addo.ai"
password = "Addo@123"
db_name="gosales"
table_name = "go_daily_sales"
output_path = "gs://dd_raw/gosales"
temp_location = "gs://tempfiles-0001"

region = "us-central1"
runner = "DirectRunner"
job_name = "mysql-to-gcs-job"

# Define pipeline options
pipeline_options = PipelineOptions(
    runner=runner,
    project=project,
    region=region,
    temp_location=temp_location,
    job_name=job_name
)


#Function to convert Beam Row object to a dictionary
def row_to_dict(row):
    return {
        'id': int(row.id),
        'retailer_code': str(row.retailer_code),
        'product_number': str(row.product_number),
        'order_method_code': str(row.order_method_code),
        'sale_date': str(row.sale_date),  # Ensure date is string or use proper datetime format
        'quantity': int(row.quantity),
        'unit_price': float(row.unit_price),
        'unit_sale_price': float(row.unit_sale_price)
    }

# # Define Parquet schema with correct types
parquet_schema = pyarrow.schema([
    ('id', pyarrow.int64()),
    ('retailer_code', pyarrow.string()),
    ('product_number', pyarrow.string()),
    ('order_method_code', pyarrow.string()),
    ('sale_date', pyarrow.string()),  # Store as string or change to pyarrow.timestamp('s')
    ('quantity', pyarrow.int64()),
    ('unit_price', pyarrow.float64()),
    ('unit_sale_price', pyarrow.float64())
])

def run():
    
    sql_query = f"""
    SELECT 
        id, retailer_code, product_number, order_method_code, 
        cast(sale_date as char) AS sale_date, 
        quantity, unit_price, unit_sale_price 
    FROM {db_name}.{table_name}
    """

    with beam.Pipeline(options=pipeline_options) as pipeline:
        mysql_data = (
            pipeline 
            | 'Read from MySQL' >> ReadFromJdbc(
                driver_class_name='com.mysql.cj.jdbc.Driver',
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                query=sql_query,
                table_name=db_name+'.'+table_name
            )
            | 'Convert Row to Dict' >> beam.Map(row_to_dict)
        )
        
        # Write data to Parquet format in GCS
        mysql_data | 'Write to Parquet' >> WriteToParquet(
            file_path_prefix=output_path+'/'+table_name +'/'+ table_name,
            schema=parquet_schema,
            file_name_suffix='.parquet'
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()