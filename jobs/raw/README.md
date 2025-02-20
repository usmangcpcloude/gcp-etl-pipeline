## Command for initiating job

```
python spanner-to-gcs-gosales-go-daily-sales.py \
    --project="kinetic-star-451310-s6" \
    --project_id="kinetic-star-451310-s6" \
    --instance_id="demo-spanner-instance" \
    --database_id="gosales" \
    --table_name="gosales.go_daily_sales" \
    --output_path="gs://dd_raw/gosales" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --temp_location="gs://tempfiles-0001" \
    --setup_file=./setup.py \
    --job_name="spanner-gosales-go_daily_sales_to_gcs"
```