##############################################################################
# storage/main.tf
# Azure Data Lake Storage Gen2 module
##############################################################################

# Random suffix for unique names
resource "random_id" "suffix" {
  byte_length = 3
  keepers = {
    region = var.location
  }
}

locals {
  name_suffix = "${var.prefix}${random_id.suffix.hex}"
}

# Storage Account
resource "azurerm_storage_account" "datalake" {
  name                     = "adls${local.name_suffix}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"

  is_hns_enabled                  = true # Enable hierarchical namespace for Delta Lake
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = var.tags
}

# Container
resource "azurerm_storage_container" "rawdata" {
  name                  = "rawdata"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Key Vault
resource "azurerm_key_vault" "main" {
  name                = "kv-${local.name_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = var.tenant_id
  sku_name            = "standard"

  enable_rbac_authorization  = false
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  network_acls {
    default_action = "Allow"
    bypass         = "AzureServices"
  }

  tags = var.tags
}

# Key Vault Access Policy for Terraform SP
resource "azurerm_key_vault_access_policy" "terraform_sp" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = var.tenant_id
  object_id    = var.terraform_sp_object_id

  secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
}

resource "time_sleep" "wait_for_kv_policy" {
  depends_on      = [azurerm_key_vault_access_policy.terraform_sp]
  create_duration = "30s"
}

# Store secrets
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
