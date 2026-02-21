# ============================================
# GOLD LAYER - Business Aggregations
# Azure Databricks PySpark Notebook
# Project: Azure End-to-End Data Pipeline
# Author: Abhijeet Bhaskar
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()

# ── ADLS Gen2 Storage Config ──
storage_account = "adlsabhijeet2026"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/processed/sales_silver"
gold_path   = f"abfss://gold@{storage_account}.dfs.core.windows.net/processed/"

# ── Read Silver Layer ──
silver_df = spark.read.format("parquet").load(silver_path)
print(f"=== SILVER RECORDS LOADED: {silver_df.count()} ===")

# ── Gold Table 1: Sales by Region ──
sales_by_region = silver_df.groupBy("Region") \
    .agg(
        sum("SalesAmount").alias("TotalRevenue"),
        count("OrderID").alias("TotalOrders"),
        avg("SalesAmount").alias("AvgOrderValue"),
        sum("Quantity").alias("TotalQuantity")
    ) \
    .withColumn("RevenuePercentage",
        round(col("TotalRevenue") / sum("TotalRevenue").over(
            __import__('pyspark.sql.window', fromlist=['Window']).Window.partitionBy()) * 100, 2)) \
    .orderBy(desc("TotalRevenue"))

print("=== SALES BY REGION ===")
sales_by_region.show()

# ── Gold Table 2: Monthly Revenue Trend ──
monthly_trend = silver_df \
    .withColumn("Year", year(col("OrderDate"))) \
    .withColumn("Month", month(col("OrderDate"))) \
    .groupBy("Year", "Month") \
    .agg(
        sum("SalesAmount").alias("MonthlyRevenue"),
        count("OrderID").alias("MonthlyOrders")
    ) \
    .orderBy("Year", "Month")

print("=== MONTHLY REVENUE TREND ===")
monthly_trend.show()

# ── Gold Table 3: Sales Summary ──
sales_summary = silver_df.agg(
    sum("SalesAmount").alias("TotalRevenue"),
    count("OrderID").alias("TotalOrders"),
    avg("SalesAmount").alias("AvgOrderValue"),
    max("SalesAmount").alias("MaxOrder"),
    min("SalesAmount").alias("MinOrder"),
    countDistinct("Region").alias("TotalRegions")
)

print("=== OVERALL SALES SUMMARY ===")
sales_summary.show()

# ── Write Gold Tables ──
sales_by_region.write.format("parquet").mode("overwrite").save(gold_path + "sales_by_region")
monthly_trend.write.format("parquet").mode("overwrite").save(gold_path + "monthly_trend")
sales_summary.write.format("parquet").mode("overwrite").save(gold_path + "sales_summary")

print("=== ALL GOLD TABLES WRITTEN SUCCESSFULLY ===")
