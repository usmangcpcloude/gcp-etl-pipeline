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



def execute_transform(spark,input_df_raw,input_df_hlp,tgt_df):    
    input_df_raw = input_df_raw.createOrReplaceTempView('retailer_input_raw')
    input_df_hlp = input_df_hlp.createOrReplaceTempView('retailer_input_hlp')
    retailer_dim = tgt_df.createOrReplaceTempView('retailer_dim')

    retailer_dim = spark.sql("""select 
                            hlp.retailer_key as retailer_key,
                            src.retailer_type as retailer_type,
                            src.country as country,
                            'gosales' as source,
                            'I' AS oper,
                            999  as table_id    
                            from retailer_input_raw src
                            inner join retailer_input_hlp hlp on lower(src.retailer_name)=lower(hlp.retailer_name) 
                            left join retailer_dim tgt on hlp.retailer_key=tgt.retailer_key
                            where tgt.retailer_key is null
                            """
                            )
    final_df = add_meta_info(retailer_dim, batch_id)
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

    job_name = "retailer_dim"
    tabl_name = "retailer_dim"
    usecase="helpings"
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
        input_df_raw=load_csv_file(spark,RAW_BUCKET+'gosales/go_retailers.csv')
        input_df_hlp=load_parquet_file(spark,CURATED_BUCKET+'helpings/retailer_hlp')
        tgt_df = load_parquet_file(spark,TARGET_PATH)
    except Exception as e:
        print("Error Occurred while loading data from raw layer")
        

    ##############################################################################
    #                           Applying transformations                         #
    ##############################################################################

    try:
        logger.info("Started Transforming Data")
        transformed_df = execute_transform(spark,input_df_raw,input_df_hlp,tgt_df).persist(StorageLevel.MEMORY_AND_DISK)
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
            transformed_df.write.format("parquet").mode("append").save(TARGET_PATH)
        else:
            print("No More Data From Source")
        job_meta_details.JOB_STATUS = 'SUCCESS'
    except Exception as e:
        print("Error Occurred While Writing Data")
    print("Job Completed!")
