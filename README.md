# Azure Databricks Real-Time Sales Analytics Pipeline

> *"Turning raw data into business intelligence, instantly."*

This platform is an enterprise-grade solution for processing millions of sales transactions in real-time. It transforms messy, fast-moving data into clear, actionable insights using a sophisticated "sorting and cleaning" process.

---

## The Big Picture (Non-Technical Explanation)

Imagine a massive, global post office where millions of letters (sales records) arrive every minute from stores all over the world. 

### The Problem
If you just pile all those letters on the floor, you can't read them, you can't count them, and you definitely can't use them to make smart business decisions. 

### The Solution: Our Digital Pipeline
This project builds a state-of-the-art **sorting facility** that works automatically:

1. **The High-Volume Intake (Event Hubs)**: The loading dock that handles millions of messages without dropping any.
2. **The Medallion Sorting Rooms**:
   - **Bronze (The Raw Pile)**: Save everything exactly as it arrived - messy but safe.
   - **Silver (The Scrubbing)**: Fix typos, remove duplicates, validate the math.
   - **Gold (The Executive Briefing)**: Summarize into reports showing bestsellers, top regions, etc.

**In short: We take the chaos of millions of individual sales and turn them into clear, smart answers.**

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   EVENT HUBS   │────▶│    DATABRICKS   │────▶│    ADLS GEN2    │
│  (Kafka API)   │     │  (Spark/Kafka) │     │  (Delta Lake)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                       │
                               ▼                       ▼
                        ┌─────────────┐         ┌─────────────┐
                        │  SQL DB     │         │   DASHBOARD │
                        │ (Enrichment)│         │ (Analytics) │
                        └─────────────┘         └─────────────┘
```

### Technology Stack

| Technology | Purpose |
|------------|---------|
| Azure Event Hubs | Real-time streaming (Kafka-compatible) |
| Azure Databricks | Spark-based analytics |
| Azure Data Lake Storage Gen2 | Scalable storage with Delta Lake |
| Azure SQL Database | Customer data enrichment |
| Delta Lake | ACID transactions on data lake |
| Terraform | Infrastructure as Code |

---

## Medallion Architecture

### 🥉 Bronze Layer (Raw Data)
- Landing zone for raw, unprocessed data
- Format: JSON from Event Hubs
- All raw events preserved

### 🥈 Silver Layer (Cleaned & Enriched)
- Cleaned, validated, enriched data
- Schema validation, deduplication
- Consistent data for downstream use

### 🥇 Gold Layer (Business Metrics)
- Aggregated, business-ready tables
- `daily_revenue` - Revenue by region and date
- `top_products` - Best-selling products
- `customer_ltv` - Customer lifetime value
- `regional_performance` - Regional metrics

---

## Infrastructure Components

### Azure Resources (via Terraform)

| Resource | Name |
|----------|------|
| Resource Group | `rg-rtsae23474-dev` |
| Storage Account | `adlsrtsae23474` |
| Blob Container | `rawdata` |
| Event Hubs Namespace | `evhns-rtsae23474` |
| Event Hub | `orders` |
| SQL Server | `dbsqle23474` |
| SQL Database | `salesdb_enriched` |
| Key Vault | `kv-rtsae23474` |

### Databricks Resources

| Resource | Details |
|----------|---------|
| Workspace | `https://adb-7405617625659224.4.azuredatabricks.net` |
| Cluster | `0224-183514-zbo8sv0g` |
| Job ID | `230840096328427` |

---

## Data Flow

```
1. DATA GENERATION
   └─> fake_data_generator.py → creates JSONL orders

2. INGESTION (Bronze)
   └─> 01_bronze_streaming.py
       - Reads from Event Hubs via Kafka
       - Parses JSON to structured data
       - Adds metadata (timestamp, event_date)
       - Writes to Delta Lake Bronze

3. TRANSFORMATION (Silver)
   └─> 02_silver_transformation.py
       - Validates and cleans data
       - Removes duplicates
       - Adds derived columns
       - Writes to Delta Lake Silver

4. AGGREGATION (Gold)
   └─> 03_gold_aggregation.py
       - Creates aggregated metrics
       - Computes daily revenue, top products, CLV
       - Writes to Delta Lake Gold

5. ENRICHMENT (SQL)
   └─> 04_jdbc_enrichment.py
       - Joins with SQL customer data
       - Adds customer demographics

6. DASHBOARD
   └─> sales_dashboard.py
       - Displays key metrics
       - Visualizations
```

---

## Pipeline Tasks (Execution Order)

```
ingest_bronze ──▶ transform_silver ──▶ aggregate_gold ──▶ 
    sql_enrichment ──▶ create_dashboard_views ──▶ sql_dashboard
```

---

## Deployment Guide

### Prerequisites

1. Azure Subscription with contributor access
2. GitHub Repository
3. Azure Service Principal

### Environment Variables (GitHub Secrets)

```
ARM_SUBSCRIPTION_ID
ARM_TENANT_ID
ARM_CLIENT_ID
ARM_CLIENT_SECRET
SQL_ADMIN_PASSWORD
```

### Deployment Options

#### Option 1: GitHub Actions

1. Push code to GitHub
2. Go to: https://github.com/jtestkc/azure-data-pipeline/actions
3. **Run workflow**
4. Select: `action: deploy` or `full-cycle`

#### Option 2: Local PowerShell

```powershell
# Clone and deploy
git clone https://github.com/jtestkc/azure-data-pipeline.git
cd azure-data-pipeline

# Login to Azure
az login --service-principal -u $ARM_CLIENT_ID -p $ARM_CLIENT_SECRET --tenant $ARM_TENANT_ID

# Deploy
.\deploy.ps1 -Stage 3
```

---

## Running the Pipeline

### Method 1: Databricks UI

1. Open: https://adb-7405617625659224.4.azuredatabricks.net
2. Go to **Jobs** → **Real-Time-Sales-Analytics-Full-Pipeline**
3. Click **Run Now**

### Method 2: API

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": 230840096328427}' \
  "https://adb-7405617625659224.4.azuredatabricks.net/api/2.1/jobs/run-now"
```

### Method 3: GitHub Actions

Run workflow with `action: run-pipeline`

---

## Accessing Dashboards

### Notebook Dashboard

**Location**: `/Shared/notebooks/sales_dashboard`

**URL**: https://adb-7405617625659224.4.azuredatabricks.net/?o=7405617625659224#notebook/workspace/Shared/notebooks/sales_dashboard

**Metrics**:
- 💰 Total Revenue
- 📦 Total Orders  
- 👥 Unique Customers
- 📍 Regional Performance
- 🏆 Top Products
- 📈 Daily Revenue Trend

### SQL Dashboard (Lakeview)

**URL**: https://adb-7405617625659224.4.azuredatabricks.net/sql/dashboards?o=7405617625659224

---

## Sending Data to Event Hubs

```python
from azure.eventhub import EventHubProducerClient, EventData

conn_str = "Endpoint=sb://evhns-rtsae23474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY"
producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name="orders")

with open('data/orders_batch_001.jsonl', 'r') as f:
    events = [EventData(line.strip()) for line in f]

with producer:
    producer.send_batch(events)
```

---

## GitHub Actions Workflow

**Location**: `.github/workflows/deploy.yml`

### Workflow Inputs

| Input | Options | Default |
|-------|---------|---------|
| environment | dev, staging, prod | dev |
| action | deploy, destroy, run-pipeline, full-cycle | deploy |
| databricks_sku | trial, standard, premium | trial |

### Actions

- **deploy**: Deploy infrastructure via Terraform
- **destroy**: Tear down resources
- **run-pipeline**: Trigger Databricks job
- **full-cycle**: Deploy + Run pipeline

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Cluster stuck in PENDING | Wait 5-10 min for Azure to provision |
| Schema merge error | Set `reset_checkpoint = true` in job |
| Event Hubs auth error | Verify connection string in Key Vault |
| Import 're' not defined | Fixed in notebook - redeploy to sync |

### Check Job Status

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://adb-7405617625659224.4.azuredatabricks.net/api/2.1/jobs/runs/list?active_only=true"
```

---

## Project Structure

```
azure-data-pipeline-project1/
├── .github/workflows/deploy.yml     # CI/CD pipeline
├── terraform/                       # Infrastructure (Terraform)
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── scripts/
│   ├── fake_data_generator.py       # Sample data
│   └── seed_data.py                 # Database seeding
├── notebooks/
│   ├── 01_ingestion/01_bronze_streaming.py    # Bronze
│   ├── 02_silver/02_silver_transformation.py  # Silver
│   ├── 03_gold/03_gold_aggregation.py        # Gold
│   ├── 04_enrichment/04_jdbc_enrichment.py   # SQL enrichment
│   └── 06_dashboard/                           # Dashboards
├── data/                             # Sample data files
├── deploy.ps1                        # Deployment script
└── README.md
```

---

## Cost Estimation

| Resource | Est. Monthly Cost |
|----------|------------------|
| Databricks (trial) | $0 (14 days), then ~$500 |
| Event Hubs (Basic) | ~$10 |
| SQL Database (Basic) | ~$5 |
| Storage (ADLS) | ~$10 |
| Key Vault | ~$1 |
| **Total** | ~$26-500/month |

---

## Additional Resources

- [Delta Lake Docs](https://docs.delta.io/)
- [Azure Databricks Docs](https://docs.microsoft.com/azure/databricks/)
- [Azure Event Hubs Docs](https://docs.microsoft.com/azure/event-hubs/)
- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/)

---

**Last Updated**: February 2026
**Version**: 1.0
