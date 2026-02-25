##############################################################################
# databricks/main.tf
##############################################################################

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${var.prefix}"
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = var.sku
  managed_resource_group_name = "rg-${var.prefix}-databricks-managed"
  tags                        = var.tags
}

resource "databricks_cluster" "main" {
  cluster_name            = "${var.prefix}-analytics-cluster"
  spark_version           = var.spark_version
  node_type_id            = var.node_type
  autotermination_minutes = var.autotermination_minutes
  num_workers             = var.num_workers
  data_security_mode      = "SINGLE_USER"

  spark_conf = {
    "fs.azure.account.key.${var.storage_account_name}.dfs.core.windows.net" = var.storage_account_key
    "spark.databricks.delta.optimizeWrite.enabled"                          = "true"
    "spark.databricks.delta.autoCompact.enabled"                            = "true"
  }

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

resource "databricks_secret_scope" "keyvault" {
  name = "kv-secrets"

  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }

  depends_on = [azurerm_databricks_workspace.main]
}

resource "databricks_notebook" "notebook_bronze" {
  source   = var.notebook_paths["bronze"]
  path     = "/Shared/notebooks/01_bronze_streaming"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_silver" {
  source   = var.notebook_paths["silver"]
  path     = "/Shared/notebooks/02_silver_transformation"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_gold" {
  source   = var.notebook_paths["gold"]
  path     = "/Shared/notebooks/03_gold_aggregation"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_enrichment" {
  source   = var.notebook_paths["enrichment"]
  path     = "/Shared/notebooks/04_jdbc_enrichment"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_dashboard" {
  source   = var.notebook_paths["dashboard"]
  path     = "/Shared/notebooks/sales_dashboard"
  language = "PYTHON"
}
