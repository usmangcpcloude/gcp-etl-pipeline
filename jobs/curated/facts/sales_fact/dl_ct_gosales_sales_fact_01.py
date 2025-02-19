from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
import sys
import re
import os
import base64
from base64 import b64encode
import json
from pyspark.sql.functions import col
from pyspark import StorageLevel


class Job_Meta_Details:
    def __init__(self, batch_id, table_id, db_name, schema_name, tbl_name, layer, rows_ingested, job_start_time, job_end_time, job_execution_time, job_status, exception, remarks, src_extraction_type, raw_ingestion_type, job_name):
        self.BATCH_ID = batch_id
        self.TABLE_ID = table_id
        self.DATABASE = db_name
        self.SCHEMA_NAME = schema_name
        self.TABLE_NAME = tbl_name
        self.LAYER = layer
        self.ROWS_INGESTED = rows_ingested
        self.JOB_START_TIME = job_start_time
        self.JOB_END_TIME = job_end_time
        self.JOB_EXECUTION_TIME = job_execution_time
        self.JOB_STATUS = job_status
        self.EXCEPTION = exception
        self.REMARKS = remarks
        self.SRC_EXTRACTION_TYPE = src_extraction_type
        self.RAW_INGESTION_TYPE = raw_ingestion_type
        self.JOB_NAME = job_name

def init_spark_session(app_name):
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()
    return spark


def load_csv_file(spark, file_path):
    df = spark.read.option("header", "true").csv(file_path)
    return df

def load_parquet_file(spark, file_path):
    df = spark.read.parquet(file_path) 
    return df


def add_meta_info(input_df, batch_id):
    input_df = input_df.withColumn('ins_tmstmp', current_timestamp())
    input_df = input_df.withColumn('upd_tmstmp', current_timestamp())
    input_df = input_df.withColumn('batch_id', lit(batch_id))
    return input_df

# Updates a job metadata dictionary with the end time of the job and the duration of the job execution time, and inserts this information into a MySQL table.



def execute_transform(spark,input_df_methods,input_df_sales,input_df_retailer_hlp,input_df_method_hlp,input_df_product_lkp):    
    methods_input = input_df_methods.createOrReplaceTempView('input_df_methods_tbl')
    sales_input = input_df_sales.createOrReplaceTempView('input_df_sales_tbl')
    retailer_hlp_input = input_df_retailer_hlp.createOrReplaceTempView('input_df_retailer_hlp_tbl')
    method_hlp_input = input_df_method_hlp.createOrReplaceTempView('input_df_method_hlp_tbl')
    product_lkp_input = input_df_product_lkp.createOrReplaceTempView('input_df_product_lkp_tbl')

    sales_fact = spark.sql("""select 
                                rh.retailer_key as retailer_key,
                                pl.product_key as product_key,
                                mh.method_key,
                                src.sale_date,
                                cast(src.quantity as int) as sell_quantity,
                                cast(pl.unit_price as float) as buying_unit_price,
                                cast(src.unit_price as float) as ask_selling_unit_price,
                                cast(src.unit_sale_price as float)selling_unit_price,
                                'gosales' as source,
                                'I' AS oper,
                                999  as table_id    
                                from (select * from input_df_sales_tbl) src
                                left join input_df_retailer_hlp_tbl rh on lower(src.retailer_code)=lower(rh.retailer_code)
                                left join input_df_product_lkp_tbl pl on lower(src.product_number)=lower(pl.product_number)
                                left join input_df_methods_tbl mt on lower(src.order_method_code)=lower(mt.order_method_code)
                                left join input_df_method_hlp_tbl mh on lower(mt.order_method_type)=lower(mh.method_name)
                                """
                                )
    final_df = add_meta_info(sales_fact, batch_id)
    return final_df

#batch_id,table_id,raw_ins_tmstmp,upd_tmstmp

if __name__ == "__main__":

    ##############################################################################
    #                        Setting Environment Variables                       #
    ##############################################################################

    # if len(sys.argv) > 4:
    #     job_name = sys.argv[1]
    #     tabl_name = sys.argv[2]
    #     usecase=sys.argv[3]
    #     env = sys.argv[4]
    #     batch_id = sys.argv[5]

    #     job_meta_details = Job_Meta_Details(batch_id, -1, None, "dd_curated", tabl_name, "curated", -1, None, None, None, None, None, None,None,None,None)
    #     #batch_id, table_id, db_name, schema_name, tbl_name, layer, rows_ingested, job_start_time, job_end_time, job_execution_time, job_status, exception, remarks
    # else:
    #     print(
    #         "ERROR: Incomplete input arguments. Please provide <job_name>,<tbl_name>,<env>,<batch_id>")
    #     exit(1)

    job_name = "sales_fact"
    tabl_name = "sales_fact"
    usecase="facts"
    env = "dev"
    batch_id = "999"

    job_meta_details = Job_Meta_Details(batch_id, -1, None, "dd_curated", tabl_name, "curated", -1, None, None, None, None, None, None,None,None,None)                
    spark = init_spark_session(job_name)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sc = spark.sparkContext
    log4j = sc._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(job_name.upper())
    logger.info("Successfully Initialized Spark Session!")

    RAW_BUCKET = "gs://dd_raw" + '/'
    CURATED_BUCKET=  "gs://dd_curated" + '/'
    TARGET_PATH=  "gs://dd_curated" + '/' + usecase +  '/' + tabl_name
    
    
    ##############################################################################
    #                             Meta Info Started                              #
    ##############################################################################

    job_start_time = datetime.now().replace(microsecond=0)
    job_meta_details.JOB_START_TIME = job_start_time
    target_table = tabl_name


    ##############################################################################
    #                           Preparing Source Query(s)                        #
    ##############################################################################

    try:
        logger.info("Started Loading Data")
        input_df_methods=load_csv_file(spark,RAW_BUCKET+'gosales/go_methods.csv')
        input_df_sales=load_csv_file(spark,RAW_BUCKET+'gosales/go_daily_sales.csv')
        input_df_retailer_hlp=load_parquet_file(spark,CURATED_BUCKET+'helpings/retailer_hlp')
        input_df_method_hlp=load_parquet_file(spark,CURATED_BUCKET+'helpings/method_hlp')
        input_df_product_lkp=load_parquet_file(spark,CURATED_BUCKET+'lookups/product_lkp')
    except Exception as e:
        print("Error Occurred while loading data from raw layer")
        print(e)
        

    ##############################################################################
    #                           Applying transformations                         #
    ##############################################################################

    try:
        logger.info("Started Transforming Data")
        transformed_df = execute_transform(spark,input_df_methods,input_df_sales,input_df_retailer_hlp,input_df_method_hlp,input_df_product_lkp).persist(StorageLevel.MEMORY_AND_DISK)
        rows_ingested = transformed_df.count()
        job_meta_details.ROWS_INGESTED = rows_ingested
    except Exception as e:
        print("Error Occurred While Transforming Data")

    ##############################################################################
    #                      Writing transformed data to hive                      #
    ##############################################################################

    try:
        logger.info("Started Writing Data")
        if rows_ingested>0:        
            transformed_df.write.format("parquet").mode("overwrite").save(TARGET_PATH)
        else:
            print("No More Data From Source")
        job_meta_details.JOB_STATUS = 'SUCCESS'
    except Exception as e:
        print("Error Occurred While Writing Data")
    print("Job Completed!")
