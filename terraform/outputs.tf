##############################################################################
# outputs.tf
# Values printed after `terraform apply` — useful for configuring notebooks.
# Beginner note: Outputs are like "return values" from Terraform. Copy these
# into your notebook configs (00_setup_and_config.py).
##############################################################################

output "resource_group_name" {
  description = "Name of the Azure Resource Group containing all resources"
  value       = azurerm_resource_group.main.name
}

output "databricks_workspace_url" {
  description = "URL to open your Databricks workspace in a browser"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  description = "Databricks workspace resource ID"
  value       = azurerm_databricks_workspace.main.id
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name (used in ABFSS paths)"
  value       = azurerm_storage_account.datalake.name
}

output "storage_container_name" {
  description = "ADLS Gen2 container name"
  value       = azurerm_storage_container.rawdata.name
}

output "storage_dfs_endpoint" {
  description = "ADLS Gen2 DFS endpoint for ABFSS paths"
  value       = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "keyvault_name" {
  description = "Key Vault name (needed to configure Databricks Secret Scope)"
  value       = azurerm_key_vault.main.name
}

output "keyvault_uri" {
  description = "Key Vault URI"
  value       = azurerm_key_vault.main.vault_uri
}

output "keyvault_resource_id" {
  description = "Key Vault resource ID (needed for Secret Scope Terraform config)"
  value       = azurerm_key_vault.main.id
}

output "eventhub_namespace_name" {
  description = "Event Hubs namespace name"
  value       = azurerm_eventhub_namespace.main.name
}

output "eventhub_name" {
  description = "Event Hub name (topic) for order events"
  value       = azurerm_eventhub.orders.name
}



output "log_analytics_workspace_id" {
  description = "Log Analytics Workspace ID for Azure Monitor"
  value       = azurerm_log_analytics_workspace.main.workspace_id
}

output "databricks_cluster_id" {
  description = "Databricks cluster ID to attach jobs/notebooks to"
  value       = databricks_cluster.main.id
}

output "secret_scope_name" {
  description = "Databricks secret scope name — use in dbutils.secrets.get()"
  value       = databricks_secret_scope.keyvault.name
}

# ── Abfss path helper ─────────────────────────────────────────────────────────
output "abfss_base_path" {
  description = "ABFSS base path for Delta tables — paste into notebooks"
  value       = "abfss://${azurerm_storage_container.rawdata.name}@${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
}

# Sensitive outputs (masked in logs)
output "storage_primary_access_key" {
  description = "Storage account primary key (stored in Key Vault as 'adls-storage-key')"
  value       = azurerm_storage_account.datalake.primary_access_key
  sensitive   = true
}

output "databricks_job_id" {
  description = "The ID of the Databricks workflow job to trigger"
  value       = databricks_job.pipeline.id
}
