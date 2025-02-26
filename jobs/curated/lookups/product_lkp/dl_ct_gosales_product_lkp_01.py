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
import os
print(os.listdir())  # Check if the files are actually present in the working directory
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
    product_input = input_df.createOrReplaceTempView('product_input_tbl')
    product_lkp = tgt_df.createOrReplaceTempView('product_lkp_tbl')

def execute_transform(spark,input_df,tgt_df):    
    product_input = input_df.createOrReplaceTempView('product_input_tbl')
    product_lkp = tgt_df.createOrReplaceTempView('product_lkp_tbl')

    product_lkp = spark.sql("""select 
                            max_key+SUM(1) OVER(ROWS UNBOUNDED PRECEDING) as product_key,
                            src.product_number as product_number,
                            src.product_line as product_line,
                            src.product_type as product_type,
                            src.product as product,
                            src.product_brand as product_brand,
                            src.product_color as product_color,
                            src.unit_cost as unit_cost,
                            src.unit_price as unit_price,
                            'gosales' as source,
                            'I' AS oper,
                            999  as table_id    
                            from (select * from product_input_tbl) src
                            left join product_lkp_tbl tgt on src.product_number=tgt.product_number
                            CROSS JOIN
                            (   
                            SELECT coalesce (MAX (product_key),0) AS max_key
                                        FROM product_lkp_tbl
                                    ) ds
                            where tgt.product_key is null
                            """
                            )
    final_df = add_meta_info(product_lkp, batch_id)
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

    job_name = "product_lkp"
    tabl_name = "product_lkp"
    usecase="lookups"
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
        input_df=load_parquet_file(spark,RAW_BUCKET+'gosales/go_products/*.parquet')
        tgt_df = load_parquet_file(spark,TARGET_PATH)
    except Exception as e:
        record_exception(Job_Meta_Details, e, "Failed during Loading Data.",MySQLConnection)
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
        record_exception(Job_Meta_Details, e, "Failed during Transforming Data.",MySQLConnection)
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
        upsert_meta_info(Job_Meta_Details, MySQLConnection)
    except Exception as e:
        record_exception(Job_Meta_Details, e, "Failed during Writing Data.",MySQLConnection)
        print("Error Occurred While Writing Data")
    print("Job Completed!")
