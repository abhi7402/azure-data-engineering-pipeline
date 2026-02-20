# 🚀 Azure End-to-End Data Engineering Pipeline

## Architecture
Raw CSV → ADF → ADLS (Bronze) → Databricks → ADLS (Silver/Gold) → Synapse → Power BI

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory | Data Ingestion |
| Azure Data Lake Gen2 | Bronze/Silver/Gold Storage |
| Azure Databricks (PySpark) | Data Transformation |
| Azure Synapse Analytics | Serverless SQL Queries |
| Power BI | Dashboard Visualization |

## Pipeline Stages
- **Bronze** — Raw CSV ingested via ADF pipeline
- **Silver** — Cleaned & transformed via Databricks PySpark
- **Gold** — Aggregated sales_summary saved as Parquet
- **Synapse** — Serverless SQL queries on Gold layer
- **Power BI** — 3 dashboard visuals

## Results
- Total Revenue: 924
- Top Region: North (41%)
- Regions: North, South, East, West

## How to Run
1. Trigger ADF pipeline to ingest raw CSV to Bronze
2. Run Databricks notebooks for Silver & Gold transformation
3. Query Gold layer in Synapse Studio
4. Refresh Power BI dashboard
