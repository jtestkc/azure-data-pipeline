##############################################################################
# storage/variables.tf
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

variable "tenant_id" {
  type      = string
  sensitive = true
}

variable "replication_type" {
  type    = string
  default = "LRS"
}

variable "tags" {
  type = map(string)
}

variable "terraform_sp_object_id" {
  type = string
}
