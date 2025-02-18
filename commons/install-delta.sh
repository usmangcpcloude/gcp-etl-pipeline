#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Define paths
SPARK_JARS_DIR="/usr/lib/spark/jars"
DELTA_VERSION="2.4.0"  # Change this to the latest version if needed

# Install Delta Lake JAR
echo "Installing Delta Lake JAR..."
gsutil cp gs://spark_jar_files_project/delta-core_2.12-2.4.0.jar $SPARK_JARS_DIR/

# Install Python dependencies for Delta Lake
echo "Installing Python dependencies..."
pip3 install delta-spark==$DELTA_VERSION

# Configure Spark defaults
echo "Configuring Spark to use Delta Lake..."
cat <<EOF >> /etc/spark/conf/spark-defaults.conf
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

echo "Delta Lake setup completed!"