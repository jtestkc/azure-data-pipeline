##############################################################################
# eventhub/variables.tf
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
  default = "Standard"
}

variable "capacity" {
  type    = number
  default = 1
}

variable "tags" {
  type = map(string)
}
