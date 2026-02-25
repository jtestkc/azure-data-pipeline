##############################################################################
# eventhub/outputs.tf
##############################################################################

output "namespace_name" {
  value = azurerm_eventhub_namespace.main.name
}

output "namespace_id" {
  value = azurerm_eventhub_namespace.main.id
}

output "eventhub_name" {
  value = azurerm_eventhub.orders.name
}

output "connection_string" {
  value     = azurerm_eventhub_namespace_authorization_rule.databricks.primary_connection_string
  sensitive = true
}
