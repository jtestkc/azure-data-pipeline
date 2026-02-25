##############################################################################
# databricks/variables.tf
##############################################################################

variable "prefix" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "sku" {
  type    = string
  default = "trial"
}

variable "spark_version" {
  type    = string
  default = "14.3.x-scala2.12"
}

variable "node_type" {
  type    = string
  default = "Standard_D4s_v3"
}

variable "num_workers" {
  type    = number
  default = 1
}

variable "autotermination_minutes" {
  type    = number
  default = 10
}

variable "storage_account_name" {
  type = string
}

variable "storage_account_key" {
  type      = string
  sensitive = true
}

variable "key_vault_id" {
  type = string
}

variable "key_vault_uri" {
  type = string
}

variable "tags" {
  type = map(string)
}

variable "notebook_paths" {
  type = map(string)
  default = {
    bronze     = ""
    silver     = ""
    gold       = ""
    enrichment = ""
    dashboard  = ""
  }
}
