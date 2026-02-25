##############################################################################
# main.tf
# Core Azure resources for the Real-Time Sales Analytics + GenAI Pipeline.
# Beginner note: This is the "blueprint" — Terraform reads this and creates
# all the cloud resources in the right order automatically.
##############################################################################

# ── Random suffix ─────────────────────────────────────────────────────────────
# Azure requires globally unique names for some resources (storage, Key Vault).
# We append a short random hex string to guarantee uniqueness.
resource "random_id" "suffix" {
  byte_length = 3 # Produces 6-character hex string e.g. "a1b2c3"
  keepers = {
    # Force fresh name suffix if we change regions (avoids soft-delete name collisions)
    region = var.location
  }
}

locals {
  # Short name used across resources: e.g. "rtsa-a1b2c3"
  name_suffix = "${var.prefix}${random_id.suffix.hex}"

  # Determine effective Databricks SKU (Azure Key Vault secret scopes require Premium)
  databricks_sku = var.databricks_sku == "trial" ? "premium" : var.databricks_sku
}

# ── Resource Group ────────────────────────────────────────────────────────────
# A Resource Group is a logical container for related Azure resources.
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name_suffix}-${var.environment}"
  location = var.location
  tags     = var.tags
}

# ============================================================================
# STORAGE — Azure Data Lake Storage Gen2 (ADLS Gen2)
# ADLS Gen2 = Azure Blob Storage with hierarchical namespace enabled.
# We store Bronze/Silver/Gold Delta tables here.
# ============================================================================
resource "azurerm_storage_account" "datalake" {
  name                     = "adls${local.name_suffix}" # Must be globally unique, lowercase, max 24 chars
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication # LRS = cheapest
  account_kind             = "StorageV2"

  # Enable hierarchical namespace = ADLS Gen2 (required for Delta Lake)
  is_hns_enabled = true

  # Security hardening
  min_tls_version = "TLS1_2"

  # Disable anonymous public access
  allow_nested_items_to_be_public = false

  blob_properties {
    delete_retention_policy {
      days = 7 # Recover accidentally deleted files within 7 days
    }
  }

  tags = var.tags
}

# Container = top-level directory in ADLS Gen2
resource "azurerm_storage_container" "rawdata" {
  name                  = "rawdata"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private" # No public access allowed
}

# ============================================================================
# KEY VAULT — Secure Secret Storage
# Stores DB passwords, connection strings, and SP credentials.
# Databricks mounts connect via Secret Scope backed by this Key Vault.
# ============================================================================
resource "azurerm_key_vault" "main" {
  name                = "kv-${local.name_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = var.tenant_id
  sku_name            = "standard" # Premium adds HSM — not needed here

  # Security: only Azure services and our SP can access
  enable_rbac_authorization  = false
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # Allow purge in dev to redeploy cleanly

  # Network policy: allow all for dev; restrict in prod
  network_acls {
    default_action = "Allow"
    bypass         = "AzureServices"
  }

  tags = var.tags
}

# Give Terraform's SP full access to manage secrets
resource "azurerm_key_vault_access_policy" "terraform_sp" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = var.tenant_id
  object_id    = data.azuread_service_principal.terraform_sp.object_id

  secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
}

resource "time_sleep" "wait_for_kv_policy" {
  depends_on      = [azurerm_key_vault_access_policy.terraform_sp]
  create_duration = "30s"
}

# ── Store secrets in Key Vault ────────────────────────────────────────────────
resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "adls-account-name"
  value        = azurerm_storage_account.datalake.name
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "adls-storage-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "eventhub_connection_string" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.databricks.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}


resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "sp-client-id"
  value        = var.client_id
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "sp-client-secret"
  value        = var.client_secret
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "sp_tenant_id" {
  name         = "sp-tenant-id"
  value        = var.tenant_id
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

# ── SQL Secrets ─────────────────────────────────────────────────────────────
resource "azurerm_key_vault_secret" "sql_username" {
  name         = "sql-username"
  value        = var.sql_admin_username
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-password"
  value        = var.sql_admin_password
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

resource "azurerm_key_vault_secret" "sql_jdbc_url" {
  name         = "sql-jdbc-url"
  value        = "jdbc:sqlserver://${azurerm_mssql_server.sql_main.fully_qualified_domain_name}:1433;database=${azurerm_mssql_database.sql_db.name};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [time_sleep.wait_for_kv_policy]
}

# ============================================================================
# EVENT HUBS — Real-Time Message Streaming
# Acts like a message queue: order events are published here and consumed
# by Spark Structured Streaming in Databricks.
# ============================================================================
resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${local.name_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = var.eventhub_sku      # "Basic" for cost savings
  capacity            = var.eventhub_capacity # 1 Throughput Unit

  tags = var.tags
}

resource "azurerm_eventhub" "orders" {
  name                = "orders"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2 # Min 2 for Basic tier
  message_retention   = 1 # Days to retain messages (Basic max = 1)
}

# Authorization rule for Databricks to read & write events
resource "azurerm_eventhub_authorization_rule" "listen_send" {
  name                = "databricks-listen-send"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.orders.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = true
  manage              = false # Least privilege: no manage permission
}

resource "azurerm_eventhub_namespace_authorization_rule" "databricks" {
  name                = "databricks-namespace-rule"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = true
  manage              = false
}

# ============================================================================
# AZURE SQL DATABASE — Reference Data & Enrichment
# ============================================================================
resource "azurerm_mssql_server" "sql_main" {
  name                         = "dbsql${random_id.suffix.hex}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = "centralus"
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  tags                         = var.tags
}

resource "azurerm_mssql_database" "sql_db" {
  name      = "salesdb_enriched"
  server_id = azurerm_mssql_server.sql_main.id
  sku_name  = "Basic"
  tags      = var.tags
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.sql_main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "local_deploy" {
  name             = "AllowLocalSeeding"
  server_id        = azurerm_mssql_server.sql_main.id
  start_ip_address = "152.59.83.148"
  end_ip_address   = "152.59.83.148"
}



# ============================================================================
# DATABRICKS WORKSPACE
# Premium SKU is required for:
#   - Unity Catalog (column masking, RLS)
#   - Secret Scopes backed by Key Vault
#   - Mosaic AI features
# ============================================================================
resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${local.name_suffix}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = local.databricks_sku
  managed_resource_group_name = "rg-${local.name_suffix}-databricks-managed"

  # Managed identity for secure access to Key Vault
  managed_services_cmk_key_vault_key_id = null # Use platform-managed keys (cost-free)



  tags = var.tags
}

# ============================================================================
# NOTE: Databricks Clusters are NOT created in Terraform
# Instead, we use JOB CLUSTERS - created automatically when job runs
# This saves cost - cluster only runs when pipeline executes
# ============================================================================

# ============================================================================
# DATABRICKS SECRET SCOPE — backed by Azure Key Vault
# Allows notebooks to fetch secrets via dbutils.secrets.get() without
# hardcoding credentials anywhere.
# ============================================================================
resource "databricks_secret_scope" "keyvault" {
  name = "kv-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.main.id
    dns_name    = azurerm_key_vault.main.vault_uri
  }

  depends_on = [
    azurerm_key_vault.main,
    time_sleep.wait_for_kv_policy,
    azurerm_databricks_workspace.main
  ]
}

# ============================================================================
# DATA SOURCES — look up existing objects (not created by Terraform)
# ============================================================================
data "azuread_service_principal" "terraform_sp" {
  client_id = var.client_id
}

# ============================================================================
# AZURE MONITOR — Log Analytics Workspace for cluster metrics
# ============================================================================
resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-${local.name_suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018" # Pay-per-GB, cheapest option
  retention_in_days   = 30          # Minimum retention period

  tags = var.tags
}



# ============================================================================
# DATABRICKS GIT CREDENTIALS
# ============================================================================
resource "databricks_git_credential" "personal_pat" {
  count                 = var.github_pat != "" ? 1 : 0
  git_provider          = "github"
  git_username          = "jtestkc"
  personal_access_token = var.github_pat
}

# ============================================================================
# DATABRICKS REPO
# ============================================================================
resource "databricks_repo" "analytics_pipeline" {
  count      = var.github_pat != "" ? 1 : 0
  url        = "https://github.com/jtestkc/azure-data-pipeline"
  path       = "/Repos/jtestkc/azure-data-pipeline"
  depends_on = [databricks_git_credential.personal_pat]
}

resource "databricks_permissions" "repo_access" {
  count   = var.github_pat != "" ? 1 : 0
  repo_id = databricks_repo.analytics_pipeline[0].id

  access_control {
    service_principal_name = var.client_id
    permission_level       = "CAN_MANAGE"
  }
}

# ============================================================================
# DATABRICKS WORKSPACE FILES (Sync local code)
# ============================================================================
resource "databricks_notebook" "notebook_bronze" {
  source   = "${path.module}/../notebooks/01_ingestion/01_bronze_streaming.py"
  path     = "/Shared/notebooks/01_bronze_streaming"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_silver" {
  source   = "${path.module}/../notebooks/02_silver/02_silver_transformation.py"
  path     = "/Shared/notebooks/02_silver_transformation"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_gold" {
  source   = "${path.module}/../notebooks/03_gold/03_gold_aggregation.py"
  path     = "/Shared/notebooks/03_gold_aggregation"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_enrichment" {
  source   = "${path.module}/../notebooks/04_enrichment/04_jdbc_enrichment.py"
  path     = "/Shared/notebooks/04_jdbc_enrichment"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_dashboard" {
  source   = "${path.module}/../notebooks/06_dashboard/sales_dashboard.py"
  path     = "/Shared/notebooks/sales_dashboard"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_sql_views" {
  source   = "${path.module}/../notebooks/06_dashboard/sql_views.py"
  path     = "/Shared/notebooks/sql_views"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_sql_dashboard" {
  source   = "${path.module}/../notebooks/06_dashboard/sql_dashboard.py"
  path     = "/Shared/notebooks/sql_dashboard"
  language = "PYTHON"
}

# ============================================================================
# GENAI NOTEBOOKS (Section 9) - Only deployed when enable_genai = true
# ============================================================================

# 9.1 Model Serving with Mosaic AI
resource "databricks_notebook" "notebook_model_serving" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/model_serving/model_serving.py"
  path     = "/Shared/notebooks/09_genai/model_serving"
  language = "PYTHON"
}

# 9.2 RAG Pipeline with Vector Search
resource "databricks_notebook" "notebook_rag_pipeline" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/rag/rag_pipeline.py"
  path     = "/Shared/notebooks/09_genai/rag_pipeline"
  language = "PYTHON"
}

# 9.3 AI Sales Agent
resource "databricks_notebook" "notebook_ai_agent" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/ai_agent/ai_sales_agent.py"
  path     = "/Shared/notebooks/09_genai/ai_sales_agent"
  language = "PYTHON"
}

# 9.4 LLM Data Quality Checker
resource "databricks_notebook" "notebook_llm_dq_checker" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/llm_checker/llm_data_quality_checker.py"
  path     = "/Shared/notebooks/09_genai/llm_dq_checker"
  language = "PYTHON"
}

# 9.5 Fine-tuning
resource "databricks_notebook" "notebook_finetuning" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/finetuning/finetuning.py"
  path     = "/Shared/notebooks/09_genai/finetuning"
  language = "PYTHON"
}

# 9.6 Governance & Guardrails
resource "databricks_notebook" "notebook_governance" {
  count = var.enable_genai ? 1 : 0

  source   = "${path.module}/../notebooks/09_genai/governance/governai_guardrails.py"
  path     = "/Shared/notebooks/09_genai/governance"
  language = "PYTHON"
}

resource "databricks_workspace_file" "config_connection" {
  source = "${path.module}/../notebooks/config/connection_config.json"
  path   = "/Shared/config/connection_config.json"
}

# ============================================================================
# DATABRICKS WORKFLOW JOBS
# Uses JOB CLUSTERS - created automatically when job runs
# This saves cost - cluster only runs when pipeline executes
# ============================================================================
resource "databricks_job" "pipeline" {
  name = "Real-Time-Sales-Analytics-Full-Pipeline"

  # Define job cluster - created when job runs (single-node)
  job_cluster {
    job_cluster_key = "pipeline_cluster"
    new_cluster {
      node_type_id  = "Standard_DC4as_v5" # AMD-based, supports Kafka and all requirements
      num_workers   = 1                   # Single node for cost savings
      spark_version = var.spark_version

      # Single node config
      spark_env_vars = {
        "SPARK_WORKER_CORES" = "4"
      }

      spark_conf = {
        "spark.databricks.delta.optimizeWrite.enabled" = "true"
        "spark.databricks.delta.autoCompact.enabled"   = "true"
      }
    }
  }

  task {
    task_key        = "ingest_bronze"
    job_cluster_key = "pipeline_cluster"
    notebook_task {
      notebook_path = databricks_notebook.notebook_bronze.path
      base_parameters = {
        "reset_checkpoint" = "true"
        "reset_data"       = "true"
      }
    }
  }

  task {
    task_key        = "transform_silver"
    job_cluster_key = "pipeline_cluster"
    depends_on {
      task_key = "ingest_bronze"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebook_silver.path
      base_parameters = {
        "reset_data" = "false"
      }
    }
  }

  task {
    task_key        = "aggregate_gold"
    job_cluster_key = "pipeline_cluster"
    depends_on {
      task_key = "transform_silver"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebook_gold.path
      base_parameters = {
        "reset_data" = "false"
      }
    }
  }

  task {
    task_key        = "sql_enrichment"
    job_cluster_key = "pipeline_cluster"
    depends_on {
      task_key = "aggregate_gold"
    }
    notebook_task {
      notebook_path = databricks_notebook.notebook_enrichment.path
      base_parameters = {
        "reset_data" = "false"
      }
    }
  }

  task {
    task_key        = "create_dashboard_views"
    job_cluster_key = "pipeline_cluster"
    depends_on {
      task_key = "sql_enrichment"
    }
    notebook_task {
      notebook_path   = databricks_notebook.notebook_sql_views.path
      base_parameters = {}
    }
  }

  task {
    task_key        = "sql_dashboard"
    job_cluster_key = "pipeline_cluster"
    depends_on {
      task_key = "create_dashboard_views"
    }
    notebook_task {
      notebook_path   = databricks_notebook.notebook_sql_dashboard.path
      base_parameters = {}
    }
  }

  email_notifications {
    on_failure = ["imjaykc31@gmail.com"]
  }
}

# ============================================================================
# GENAI WORKFLOW JOB - DISABLED FOR NOW
# Enable by setting enable_genai = true in terraform.tfvars
# ============================================================================
# resource "databricks_job" "genai_pipeline" {
#   count = var.enable_genai ? 1 : 0
#   name = "GenAI-Pipeline-Mosaic-AI"
#   # Uses job clusters - created when job runs
# }

# ============================================================================
# DATABRICKS WORKSPACE ACCESS (ADMIN)
# ============================================================================
# Configure the Databricks workspace provider for resources that need workspace URL
# Note: Default provider is configured in providers.tf
provider "databricks" {
  alias = "workspace"
  host  = azurerm_databricks_workspace.main.workspace_url

  # Authenticate using the Service Principal
  azure_client_id     = var.client_id
  azure_client_secret = var.client_secret
  azure_tenant_id     = var.tenant_id
}

# 1. Add the human user to the Databricks Workspace
resource "databricks_user" "human_admin" {
  provider  = databricks.workspace
  user_name = "vedica@wordsandagesgmail.onmicrosoft.com"

  # Standard capabilities
  workspace_access           = true
  allow_cluster_create       = true
  allow_instance_pool_create = true
  databricks_sql_access      = true
}

# 2. Add the entitlement indicating they are a workspace administrator
resource "databricks_entitlements" "human_admin_privileges" {
  provider             = databricks.workspace
  user_id              = databricks_user.human_admin.id
  workspace_access     = true
  allow_cluster_create = true

  # This explicitly grants admin access on the workspace level
  # without needing to query the volatile "admins" group
}

# Note: Service Principal was already created in previous deployment
# We create PAT token directly without needing to look up the SP first
# 4. Create PAT token for Service Principal (for GitHub Actions)
resource "databricks_token" "sp_token" {
  provider         = databricks.workspace
  comment          = "GitHub Actions Token"
  lifetime_seconds = 2592000 # 30 days
}

# Note: PAT token is available via terraform output databricks_pat_token
# GitHub Actions will retrieve it directly from terraform state

# ============================================================================
# ZERO-TOUCH CI/CD AUTOCONFIGURATION
# ============================================================================
# PAT token is now stored in Azure Key Vault (databricks-pat-token secret)
# GitHub Actions workflow will retrieve it from Key Vault after deployment

# Commented out - using Key Vault approach instead
# resource "github_actions_secret" "databricks_token" {
#   repository      = "azure-data-pipeline"
#   secret_name     = "DATABRICKS_TOKEN"
#   plaintext_value = databricks_token.sp_token.token_value
# }
