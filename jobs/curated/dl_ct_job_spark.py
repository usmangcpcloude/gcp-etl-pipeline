from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("GCSReadWrite").getOrCreate()
categories = spark.read.option("header", "true").csv("gs://dd_raw/population.csv")
output_path = "gs://dd_curated/population/"

# Write data back to GCS as Parquet (or CSV, depending on your preference)
categories.write.mode("overwrite").parquet(output_path)

print("Data written to GCS successfully!")
spark.stop()