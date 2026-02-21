# ============================================
# SILVER LAYER - Data Cleaning & Transformation
# Azure Databricks PySpark Notebook
# Project: Azure End-to-End Data Pipeline
# Author: Abhijeet Bhaskar
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverTransformation").getOrCreate()

# ── ADLS Gen2 Storage Config ──
storage_account = "adlsabhijeet2026"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/processed/sales_bronze"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/processed/sales_silver"

# ── Read Bronze Layer ──
bronze_df = spark.read.format("parquet").load(bronze_path)

print("=== BRONZE DATA LOADED ===")
print(f"Records: {bronze_df.count()}")
bronze_df.show(5)

# ── Data Cleaning ──

# 1. Drop nulls in critical columns
cleaned_df = bronze_df.dropna(subset=["OrderID", "SalesAmount", "Region"])

# 2. Remove duplicates
cleaned_df = cleaned_df.dropDuplicates(["OrderID"])

# 3. Fix data types
cleaned_df = cleaned_df \
    .withColumn("SalesAmount", col("SalesAmount").cast(DoubleType())) \
    .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
    .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))

# 4. Standardize Region column
cleaned_df = cleaned_df \
    .withColumn("Region", upper(trim(col("Region"))))

# 5. Add Silver metadata
silver_df = cleaned_df \
    .withColumn("transformation_timestamp", current_timestamp()) \
    .withColumn("layer", lit("silver")) \
    .drop("ingestion_timestamp", "source_file")

print("=== SILVER DATA SCHEMA ===")
silver_df.printSchema()
print(f"=== RECORDS AFTER CLEANING: {silver_df.count()} ===")
silver_df.show(5)

# ── Write to Silver Layer ──
silver_df.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("Region") \
    .save(silver_path)

print("=== SILVER LAYER WRITTEN SUCCESSFULLY ===")
