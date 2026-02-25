##############################################################################
# databricks/outputs.tf
##############################################################################

output "workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

output "cluster_id" {
  value = databricks_cluster.main.id
}

output "cluster_name" {
  value = databricks_cluster.main.cluster_name
}
