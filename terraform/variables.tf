##############################################################################
# variables.tf
# All input variables for the pipeline.
# Beginner note: Variables make the code reusable — you change values here
# (or in terraform.tfvars) without touching the main logic.
##############################################################################

# ── Authentication ──────────────────────────────────────────────────────────
variable "subscription_id" {
  description = "Your Azure Subscription ID (found in Azure Portal → Subscriptions)"
  type        = string
  sensitive   = true # Terraform will not print this in logs
}

variable "tenant_id" {
  description = "Your Azure Active Directory Tenant ID"
  type        = string
  sensitive   = true
}

variable "client_id" {
  description = "Service Principal Application (client) ID used by Terraform"
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Service Principal client secret"
  type        = string
  sensitive   = true
}

# ── General ─────────────────────────────────────────────────────────────────
variable "prefix" {
  description = "Short prefix for all resource names (3-6 lowercase letters)"
  type        = string
  default     = "rtsa" # Real-Time Sales Analytics
  validation {
    condition     = can(regex("^[a-z]{3,6}$", var.prefix))
    error_message = "Prefix must be 3-6 lowercase letters only."
  }
}

variable "location" {
  description = "Azure region for all resources (e.g. eastus, westeurope)"
  type        = string
  default     = "eastus" # ✅ Free tier compatible, all services available
}

variable "environment" {
  description = "Deployment environment tag (dev/staging/prod)"
  type        = string
  default     = "dev"
}

# ── Networking ───────────────────────────────────────────────────────────────
variable "allowed_ip_ranges" {
  description = "List of IPs allowed to access Key Vault and SQL (add your own IP)"
  type        = list(string)
  default     = [] # Empty = no IP restriction (adjust for production!)
}

# ── Databricks ───────────────────────────────────────────────────────────────
variable "databricks_sku" {
  description = "Databricks workspace SKU: trial (14 days free) | standard | premium"
  type        = string
  default     = "trial" # ✅ Trial for 14 days free, then standard
}

variable "cluster_node_type" {
  description = "VM size for Databricks cluster workers."
  type        = string
  default     = "Standard_DS3_v2" # ✅ Smaller = cheaper ($0.04/hr vs $0.17/hr)
}

variable "cluster_min_workers" {
  description = "Minimum worker nodes (set to 1 to save cost)"
  type        = number
  default     = 1
}

variable "cluster_max_workers" {
  description = "Maximum worker nodes for autoscaling"
  type        = number
  default     = 1 # ✅ Single node = cheaper, no driver-worker communication
}

variable "cluster_autotermination_minutes" {
  description = "Minutes of inactivity before cluster auto-terminates (saves cost!)"
  type        = number
  default     = 5 # ✅ Very aggressive - saves money when not in use
}

variable "spark_version" {
  description = "Databricks Runtime version (14.3 LTS = stable, Spark 3.4)"
  type        = string
  default     = "14.3.x-scala2.12" # ✅ 14.3 LTS - stable, long-term support
}

# ── Event Hubs ───────────────────────────────────────────────────────────────
variable "eventhub_sku" {
  description = "Event Hubs namespace tier: Basic | Standard | Premium"
  type        = string
  default     = "Standard" # ✅ REQUIRED for Kafka - Basic does NOT support Kafka protocol!
}

variable "eventhub_capacity" {
  description = "Throughput Units (1 TU = 1 MB/s in, 2 MB/s out)"
  type        = number
  default     = 1
}

# ── Storage ──────────────────────────────────────────────────────────────────
variable "storage_replication" {
  description = "Storage replication type: LRS | GRS | ZRS"
  type        = string
  default     = "LRS" # Locally Redundant = cheapest
}

# ── Azure SQL ─────────────────────────────────────────────────────────────────
variable "sql_admin_username" {
  description = "SQL Server administrator username"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "SQL Server administrator password (min 8 chars, upper+lower+digit+symbol)"
  type        = string
  sensitive   = true
}

variable "sql_database_sku" {
  description = "Azure SQL Database SKU"
  type        = string
  default     = "Free" # ✅ Free for 12 months, then Basic ~$5/mo
}

# ── Tags ─────────────────────────────────────────────────────────────────────
variable "tags" {
  description = "Tags applied to all resources for cost tracking"
  type        = map(string)
  default = {
    project     = "azure-data-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}

variable "github_pat" {
  description = "GitHub Personal Access Token for Databricks Repo connection"
  type        = string
  sensitive   = true
  default     = ""
}
