##############################################################################
# storage/outputs.tf
##############################################################################

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}

output "container_name" {
  value = azurerm_storage_container.rawdata.name
}

output "key_vault_id" {
  value = azurerm_key_vault.main.id
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}
