# Azure Deployment Guide

## Architecture

```
Azure Container Apps
  └── mtc-insights-ui (Streamlit)
      ├── Azure Container Registry (Docker image)
      ├── Azure Key Vault (secrets)
      └── Azure Databricks (data backend)
```

## Prerequisites
- Azure subscription
- Azure CLI installed and authenticated (`az login`)
- Docker installed locally
- Databricks workspace connected (see `databricks_setup.md`)

## 1. Create Azure Resources

```bash
RESOURCE_GROUP=rg-mtc-insights
LOCATION=uksouth
ACR_NAME=mtcinsightsacr
APP_ENV=mtc-insights-env

# Create resource group
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create container registry
az acr create --resource-group $RESOURCE_GROUP \
  --name $ACR_NAME --sku Basic --admin-enabled true

# Create Container Apps environment
az containerapp env create \
  --name $APP_ENV \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION
```

## 2. Build and Push Docker Image

```bash
az acr build --registry $ACR_NAME \
  --image mtc-insights:latest .
```

## 3. Deploy to Container Apps

```bash
az containerapp create \
  --name mtc-insights-ui \
  --resource-group $RESOURCE_GROUP \
  --environment $APP_ENV \
  --image $ACR_NAME.azurecr.io/mtc-insights:latest \
  --target-port 8501 \
  --ingress external \
  --min-replicas 1 \
  --max-replicas 3 \
  --secrets \
    openai-key=<OPENAI_API_KEY> \
    databricks-token=<DATABRICKS_TOKEN> \
  --env-vars \
    OPENAI_API_KEY=secretref:openai-key \
    DATABRICKS_HOST=<your-host> \
    DATABRICKS_HTTP_PATH=<your-path> \
    DATABRICKS_TOKEN=secretref:databricks-token
```

## 4. Verify Deployment

```bash
az containerapp show \
  --name mtc-insights-ui \
  --resource-group $RESOURCE_GROUP \
  --query "properties.configuration.ingress.fqdn"
```

Open the URL in your browser to access the Streamlit UI.

## 5. Environment Variables Reference

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENAI_API_KEY` | OpenAI API key | Yes |
| `DATABRICKS_HOST` | Databricks workspace host | Yes |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path | Yes |
| `DATABRICKS_TOKEN` | Databricks PAT token | Yes |
| `VECTOR_STORE_PATH` | Path to FAISS index | No (default: /tmp) |
| `LOG_LEVEL` | Logging level | No (default: INFO) |

## Security Notes
- Use Azure Key Vault for production secrets
- Enable managed identity for the Container App
- Restrict Container Registry access to the Container App only
- Enable Databricks IP access lists
