##############################################################################
# genai/variables.tf
##############################################################################

variable "prefix" {
  type = string
}

variable "spark_version" {
  type    = string
  default = "14.3.x-scala2.12"
}

variable "node_type" {
  type    = string
  default = "Standard_DS3_v2"
}

variable "num_workers" {
  type    = number
  default = 1
}

variable "autotermination_minutes" {
  type    = number
  default = 15
}

variable "storage_account_name" {
  type = string
}

variable "storage_account_key" {
  type      = string
  sensitive = true
}

variable "databricks_workspace_id" {
  type = string
}

variable "notebook_paths" {
  type = map(string)
  default = {
    model_serving = ""
    rag           = ""
    ai_agent      = ""
    llm_checker   = ""
    finetuning    = ""
    governance    = ""
  }
}
