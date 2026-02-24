##############################################################################
# providers.tf
# Declares all required Terraform providers and sets version constraints.
# Beginner note: Providers are plugins that let Terraform talk to cloud APIs.
##############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    # AzureRM — main Azure resource manager
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    # Azure Active Directory — for Service Principals
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    # Databricks — workspace-level resources (clusters, notebooks, jobs)
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.36"
    }
    # Random — generates unique suffixes to avoid naming collisions
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# ── Azure Provider ──────────────────────────────────────────────────────────
provider "azurerm" {
  features {
    key_vault {
      # Keep soft-deleted secrets recoverable for 7 days (cost-safe)
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }

  # Credentials come from environment variables or terraform.tfvars:
  # ARM_SUBSCRIPTION_ID, ARM_TENANT_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
  client_id       = var.client_id
  client_secret   = var.client_secret
}

# ── Azure AD Provider ───────────────────────────────────────────────────────
provider "azuread" {
  tenant_id     = var.tenant_id
  client_id     = var.client_id
  client_secret = var.client_secret
}

# ── Databricks Provider ─────────────────────────────────────────────────────
# Configures after the workspace is created (uses workspace URL from output)
provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
  # Authenticates via Azure CLI / Service Principal automatically
  azure_workspace_resource_id = azurerm_databricks_workspace.main.id
  azure_client_id             = var.client_id
  azure_client_secret         = var.client_secret
  azure_tenant_id             = var.tenant_id
}
