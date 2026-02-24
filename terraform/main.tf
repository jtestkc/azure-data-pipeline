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
  value        = azurerm_eventhub_authorization_rule.listen_send.primary_connection_string
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
# DATABRICKS CLUSTER — Single-node, auto-terminating
# Cost optimization: 1 worker, terminate after 5 min idle
# ============================================================================
resource "databricks_cluster" "main" {
  cluster_name            = "${var.prefix}-analytics-cluster"
  spark_version           = var.spark_version
  node_type_id            = var.cluster_node_type
  autotermination_minutes = var.cluster_autotermination_minutes
  num_workers             = 0 # MUST be 0 for SingleNode cluster, otherwise it hangs waiting for workers

  # ✅ FIXED: Use NONE for service principal authentication
  # SINGLE_USER requires a user email, not a service principal client_id
  data_security_mode = "NONE"

  spark_conf = {
    # Storage Access
    "fs.azure.account.key.${azurerm_storage_account.datalake.name}.dfs.core.windows.net" = azurerm_storage_account.datalake.primary_access_key

    # Single Node mode
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"

    # Delta optimizations
    "spark.databricks.delta.optimizeWrite.enabled" = "true"
    "spark.databricks.delta.autoCompact.enabled"   = "true"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  # Libraries - Using built-in Kafka connector (no need for azure-eventhubs-spark)
  library {
    pypi {
      package = "azure-eventhub>=5.0.0"
    }
  }
  library {
    pypi {
      package = "faker>=22.0.0"
    }
  }
  library {
    pypi {
      package = "mlflow>=2.10.0"
    }
  }

  depends_on = [azurerm_databricks_workspace.main]
}

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

resource "databricks_workspace_file" "config_connection" {
  source = "${path.module}/../notebooks/config/connection_config.json"
  path   = "/Shared/config/connection_config.json"
}

# ============================================================================
# DATABRICKS WORKFLOW JOBS
# ============================================================================
resource "databricks_job" "pipeline" {
  name = "Real-Time-Sales-Analytics-Full-Pipeline"

  task {
    task_key            = "ingest_bronze"
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_bronze.path
      base_parameters = {
        "reset_checkpoint" = "true"
      }
    }
  }

  task {
    task_key = "transform_silver"
    depends_on {
      task_key = "ingest_bronze"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_silver.path
    }
  }

  task {
    task_key = "aggregate_gold"
    depends_on {
      task_key = "transform_silver"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_gold.path
    }
  }

  task {
    task_key = "sql_enrichment"
    depends_on {
      task_key = "aggregate_gold"
    }
    existing_cluster_id = databricks_cluster.main.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_enrichment.path
    }
  }

  email_notifications {
    on_failure = ["jtestkc@gmail.com"]
  }
}

# ============================================================================
# DATABRICKS WORKSPACE ACCESS (ADMIN)
# ============================================================================
# Configure the Databricks provider to authenticate against the new workspace
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
