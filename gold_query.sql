-- ============================================
-- SYNAPSE ANALYTICS - Gold Layer SQL Queries
-- Project: Azure End-to-End Data Pipeline
-- Author: Abhijeet Bhaskar
-- ============================================

-- ── Create External Data Source ──
CREATE EXTERNAL DATA SOURCE GoldStorage
WITH (
    LOCATION = 'abfss://gold@adlsabhijeet2026.dfs.core.windows.net/processed/'
);

-- ── Query 1: Sales by Region ──
SELECT * FROM
OPENROWSET(
    BULK 'sales_by_region/*.parquet',
    DATA_SOURCE = 'GoldStorage',
    FORMAT = 'PARQUET'
) AS sales_region
ORDER BY TotalRevenue DESC;

-- ── Query 2: Monthly Revenue Trend ──
SELECT
    Year,
    Month,
    MonthlyRevenue,
    MonthlyOrders,
    ROUND(MonthlyRevenue / LAG(MonthlyRevenue) OVER (ORDER BY Year, Month) * 100 - 100, 2) AS GrowthPercent
FROM
OPENROWSET(
    BULK 'monthly_trend/*.parquet',
    DATA_SOURCE = 'GoldStorage',
    FORMAT = 'PARQUET'
) AS monthly
ORDER BY Year, Month;

-- ── Query 3: Overall Sales Summary ──
SELECT * FROM
OPENROWSET(
    BULK 'sales_summary/*.parquet',
    DATA_SOURCE = 'GoldStorage',
    FORMAT = 'PARQUET'
) AS summary;

-- ── Query 4: Top Performing Region ──
SELECT TOP 1
    Region,
    TotalRevenue,
    TotalOrders,
    RevenuePercentage
FROM
OPENROWSET(
    BULK 'sales_by_region/*.parquet',
    DATA_SOURCE = 'GoldStorage',
    FORMAT = 'PARQUET'
) AS top_region
ORDER BY TotalRevenue DESC;
