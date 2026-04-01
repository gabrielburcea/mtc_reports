# Databricks Deployment Guide

## Prerequisites
- Databricks workspace (Azure / AWS / GCP)
- Unity Catalog enabled
- SQL Warehouse (Serverless or Pro tier recommended)
- Service principal or Personal Access Token (PAT)

## 1. Create the Unity Catalog

```sql
CREATE CATALOG IF NOT EXISTS edu_insights;
USE CATALOG edu_insights;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS metadata;
```

## 2. Run the Metadata Repository DDL

In the Databricks SQL Editor, run the contents of `sql/metadata/01_metadata_repository.sql`.

## 3. Create Bronze Layer Tables

Run each file in `sql/ddl/` to create the raw source tables:
```bash
# From Databricks CLI
databricks fs cp sql/ddl/ dbfs:/mnt/sql/ddl/ -r
```

## 4. Create Silver Layer Views

Run each file in `sql/views/` to create the decoded views.

## 5. Configure Authentication

### Option A: Personal Access Token (PAT)
1. Go to User Settings → Access Tokens
2. Generate a new token
3. Set `DATABRICKS_TOKEN=<your-token>` in your `.env` file

### Option B: Service Principal
1. Create a service principal in Azure AD
2. Grant it `CAN USE` on the SQL Warehouse
3. Grant it `SELECT` on all Unity Catalog tables
4. Set `DATABRICKS_TOKEN=<service-principal-token>`

## 6. Configure the Connector

```python
from src.integrations.databricks_connector import DatabricksConfig, DatabricksConnector

config = DatabricksConfig(
    host="adb-<workspace-id>.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/<warehouse-id>",
    access_token="<your-token>",
    catalog="edu_insights",
)
connector = DatabricksConnector(config=config)
```

## 7. Test the Connection

```python
result = connector.execute_query("SELECT current_catalog(), current_user()")
print(result.data)
```

## 8. Build the Vector Store

```python
from src.rag.embedding_pipeline import EmbeddingPipeline

pipeline = EmbeddingPipeline(openai_api_key="<key>")
pipeline.build()
pipeline.save("/dbfs/mnt/vector_store")
```
