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
