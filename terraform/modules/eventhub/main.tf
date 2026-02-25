##############################################################################
# eventhub/main.tf
##############################################################################

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${var.prefix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = var.sku
  capacity            = var.capacity

  tags = var.tags
}

resource "azurerm_eventhub" "orders" {
  name                = "orders"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = var.resource_group_name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_authorization_rule" "listen_send" {
  name                = "databricks-listen-send"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.orders.name
  resource_group_name = var.resource_group_name
  listen              = true
  send                = true
  manage              = false
}

resource "azurerm_eventhub_namespace_authorization_rule" "databricks" {
  name                = "databricks-namespace-rule"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = var.resource_group_name
  listen              = true
  send                = true
  manage              = false
}
