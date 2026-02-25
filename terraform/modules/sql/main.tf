##############################################################################
# sql/main.tf
##############################################################################

resource "azurerm_mssql_server" "main" {
  name                         = "dbsql${var.prefix}"
  resource_group_name          = var.resource_group_name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.admin_username
  administrator_login_password = var.admin_password
  tags                         = var.tags
}

resource "azurerm_mssql_database" "db" {
  name      = "salesdb_enriched"
  server_id = azurerm_mssql_server.main.id
  sku_name  = var.sku_name
  tags      = var.tags
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "local_deploy" {
  name             = "AllowLocalMachine"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = var.allowed_ip
  end_ip_address   = var.allowed_ip
}
