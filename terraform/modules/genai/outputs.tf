##############################################################################
# genai/outputs.tf
##############################################################################

output "genai_cluster_id" {
  value = databricks_cluster.genai.id
}

output "genai_cluster_name" {
  value = databricks_cluster.genai.cluster_name
}

output "genai_job_id" {
  value = databricks_job.genai_pipeline.id
}

output "genai_job_name" {
  value = databricks_job.genai_pipeline.name
}
