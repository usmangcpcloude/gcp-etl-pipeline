import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.spanner import ReadFromSpanner
from apache_beam.io import WriteToText
import logging
import argparse
import datetime
from typing import Optional

# import unicode
import sys
import codecs

from typing import NamedTuple
from apache_beam import coders

service_account_json = "service-account-compute-addo.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json


class ExampleRow(NamedTuple):
    id: int
    retailer_code: str
    product_number: str
    order_method_code: str
    sale_date: str
    quantity: int
    unit_price: float
    unit_sale_price: float

coders.registry.register_coder(ExampleRow, coders.RowCoder)

class SpannerToGCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add custom arguments
        parser.add_argument(
            '--project_id',
            required=True,
            help='GCP Project ID'
        )
        parser.add_argument(
            '--instance_id',
            required=True,
            help='Spanner Instance ID'
        )
        parser.add_argument(
            '--database_id',
            required=True,
            help='Spanner Database ID'
        )
        parser.add_argument(
            '--table_name',
            required=True,
            help='Spanner Table Name to read from'
        )
        parser.add_argument(
            '--output_path',
            required=True,
            help='GCS Output path (e.g., gs://bucket-name/path/to/output)'
        )

def dict_to_csv(row):
    # Convert NamedTuple to CSV string
    return ','.join(str(value) for value in row)

def run():
    # Parse arguments
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    
    # Create pipeline options
    options = SpannerToGCSOptions(pipeline_args)
    
    # Create and run the pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read from Spanner using SQL query
        # sql_query = f'SELECT employee_id, first_name, last_name, email, hire_date FROM {options.table_name}'
        sql_query = f"""
        SELECT
            id,
            retailer_code::text AS retailer_code,
            product_number::text AS product_number,
            order_method_code::text AS order_method_code,
            sale_date::text AS sale_date,
            quantity::int AS quantity,
            unit_price::float AS unit_price,
            unit_sale_price::float AS unit_sale_price
        FROM {options.table_name}
        """
        spanner_data = pipeline | 'Read from Spanner' >> ReadFromSpanner(
            project_id=options.project_id,
            instance_id=options.instance_id,
            database_id=options.database_id,
            row_type=ExampleRow,
            sql=sql_query
            # table=options.table_name
        )
        
        # Convert dictionary to CSV format
        csv_data = spanner_data | 'Convert to CSV' >> beam.Map(dict_to_csv)
        
        # Write to GCS
        csv_data | 'Write to GCS' >> WriteToText(
            file_path_prefix=options.output_path + '/go_daily_sales',
            file_name_suffix='.csv',
            # give header
            header='id,retailer_code,product_number,order_method_code,sale_date,quantity,unit_price,unit_sale_price'
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
