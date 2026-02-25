##############################################################################
# sql/outputs.tf
##############################################################################

output "server_name" {
  value = azurerm_mssql_server.main.name
}

output "server_id" {
  value = azurerm_mssql_server.main.id
}

output "database_name" {
  value = azurerm_mssql_database.db.name
}

output "jdbc_url" {
  value = "jdbc:sqlserver://${azurerm_mssql_server.main.fully_qualified_domain_name}:1433;database=${azurerm_mssql_database.db.name};encrypt=true;trustServerCertificate=false"
}
