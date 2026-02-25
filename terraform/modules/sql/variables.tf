##############################################################################
# sql/variables.tf
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

variable "admin_username" {
  type      = string
  sensitive = true
}

variable "admin_password" {
  type      = string
  sensitive = true
}

variable "sku_name" {
  type    = string
  default = "Free"
}

variable "allowed_ip" {
  type    = string
  default = "0.0.0.0"
}

variable "tags" {
  type = map(string)
}
