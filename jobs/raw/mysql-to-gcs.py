import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io import WriteToText
import logging
import argparse

# Set Google Cloud credentials
service_account_json = "commons/service-account-compute-addo.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json

class MySQLToGCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--jdbc_url', required=True, help='JDBC URL for MySQL connection')
        parser.add_argument('--username', required=True, help='MySQL username')
        parser.add_argument('--password', required=True, help='MySQL password')
        parser.add_argument('--table_name', required=True, help='MySQL Table Name to read from')
        parser.add_argument('--output_path', required=True, help='GCS Output path')

# Function to convert Beam Row object to a dictionary and handle NULLs
def row_to_dict(row):
    return row._asdict()

# Convert dictionary to CSV format
def dict_to_csv(row):
    return ','.join(str(row[key]) for key in ['id', 'retailer_code', 'product_number', 
                                              'order_method_code', 'sale_date', 
                                              'quantity', 'unit_price', 'unit_sale_price'])

def run():
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    
    options = MySQLToGCSOptions(pipeline_args)
    
    sql_query = f"""
    SELECT 
        id, retailer_code, product_number, order_method_code, 
        COALESCE(sale_date, '1970-01-01') AS sale_date, 
        quantity, unit_price, unit_sale_price 
    FROM {options.table_name}
    """

    with beam.Pipeline(options=options) as pipeline:
        mysql_data = (
            pipeline 
            | 'Read from MySQL' >> ReadFromJdbc(
                driver_class_name='com.mysql.cj.jdbc.Driver',
                jdbc_url=options.jdbc_url,
                username=options.username,
                password=options.password,
                query=sql_query,
                table_name=options.table_name
            )
            | 'Convert Row to Dict' >> beam.Map(row_to_dict)
        )

        # Debug: Print Read Data
        #mysql_data | 'Print Read Data' >> beam.Map(lambda row: print("Read row:", row))

        # Convert to CSV format
        csv_data = mysql_data | 'Convert to CSV' >> beam.Map(dict_to_csv)

        # Write CSV to GCS
        csv_data | 'Write to GCS' >> WriteToText(
            file_path_prefix=options.output_path + '/go_daily_sales',
            file_name_suffix='.csv',
            header='id,retailer_code,product_number,order_method_code,sale_date,quantity,unit_price,unit_sale_price'
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
