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
from db_configs import *
from utilities import *
from env_variables import variables
from Job_Meta_Details import Job_Meta_Details

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



def execute_transform(spark,input_df,tgt_df):    
    retailer_input = input_df.createOrReplaceTempView('retailer_input_tbl')
    retailer_hlp = tgt_df.createOrReplaceTempView('retailer_hlp_tbl')

    retailer_hlp = spark.sql("""select 
                            max_key+SUM(1) OVER(ROWS UNBOUNDED PRECEDING) as retailer_key,
                            src.retailer_code as retailer_code,
                            'gosales' as source,
                            'I' AS oper,
                            999  as table_id    
                            from (select distinct retailer_code from retailer_input_tbl) src
                            left join retailer_hlp_tbl tgt on lower(src.retailer_code)=lower(tgt.retailer_code)
                            CROSS JOIN
                            (   
                            SELECT coalesce (MAX (retailer_key),0) AS max_key
                                        FROM retailer_hlp_tbl
                                    ) ds
                            where tgt.retailer_key is null
                            """
                            )
    final_df = add_meta_info(retailer_hlp, batch_id)
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

    job_name = "retailer_hlp"
    tabl_name = "retailer_hlp"
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
    project,region,mysql_etl_monitoring=env_configs(env)
    print("Trying to Open MYSQL connection",flush =True)
    MySQLConnection=openMySQLConnection(mysql_etl_monitoring,project)
    print("MySQLConnection connection successful", flush =True)

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
        input_df=load_parquet_file(spark,RAW_BUCKET+'gosales/go_retailers/*.parquet')
        tgt_df = load_parquet_file(spark,TARGET_PATH)
    except Exception as e:
        record_exception(job_meta_details, e, "Failed during Loading Data.",MySQLConnection)
        print("Error Occurred while loading data from raw layer")
        

    ##############################################################################
    #                           Applying transformations                         #
    ##############################################################################

    try:
        logger.info("Started Transforming Data")
        transformed_df = execute_transform(spark,input_df,tgt_df).persist(StorageLevel.MEMORY_AND_DISK)
        rows_ingested = transformed_df.count()
        job_meta_details.ROWS_INGESTED = rows_ingested
    except Exception as e:
        record_exception(job_meta_details, e, "Failed during Transforming Data.",MySQLConnection)
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
        upsert_meta_info(job_meta_details, MySQLConnection)
    except Exception as e:
        record_exception(job_meta_details, e, "Failed during Writing Data.",MySQLConnection)
        print("Error Occurred While Writing Data")
    print("Job Completed!")
