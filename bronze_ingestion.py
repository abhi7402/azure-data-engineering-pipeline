# ============================================
# BRONZE LAYER - Raw Data Ingestion
# Azure Databricks PySpark Notebook
# Project: Azure End-to-End Data Pipeline
# Author: Abhijeet Bhaskar
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# ── ADLS Gen2 Storage Config ──
storage_account = "adlsabhijeet2026"
container_bronze = "bronze"
adls_path = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/"

# ── Read Raw CSV from Bronze Container ──
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(adls_path + "raw/sales_data.csv")

print("=== RAW DATA SCHEMA ===")
raw_df.printSchema()

print(f"=== TOTAL RECORDS: {raw_df.count()} ===")
raw_df.show(5)

# ── Add Metadata Columns ──
bronze_df = raw_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("sales_data.csv")) \
    .withColumn("layer", lit("bronze"))

# ── Write to Bronze Layer as Parquet ──
bronze_df.write.format("parquet") \
    .mode("overwrite") \
    .save(adls_path + "processed/sales_bronze")

print("=== BRONZE LAYER WRITTEN SUCCESSFULLY ===")
print(f"Records written: {bronze_df.count()}")
