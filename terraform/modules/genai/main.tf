##############################################################################
# genai/main.tf
# Optional GenAI module - only deployed when enable_genai = true
##############################################################################

# GenAI Cluster with Spot Instances for cost savings
resource "databricks_cluster" "genai" {
  cluster_name            = "${var.prefix}-genai-cluster"
  spark_version           = var.spark_version
  node_type_id            = var.node_type
  autotermination_minutes = var.autotermination_minutes
  num_workers             = var.num_workers
  data_security_mode      = "SINGLE_USER"

  # Enable spot instances for 60-70% cost savings
  azure_attributes {
    availability = "SPOT_AZURE"
  }

  spark_conf = {
    "fs.azure.account.key.${var.storage_account_name}.dfs.core.windows.net" = var.storage_account_key
    "spark.databricks.delta.optimizeWrite.enabled"                          = "true"
    "spark.databricks.delta.autoCompact.enabled"                            = "true"
    "spark.mlflow.trackingUri"                                              = "databricks"
  }

  library {
    pypi {
      package = "mlflow>=2.10.0"
    }
  }
  library {
    pypi {
      package = "langchain>=0.1.0"
    }
  }
  library {
    pypi {
      package = "databricks-vector-search"
    }
  }

  depends_on = [var.databricks_workspace_id]
}

# GenAI Notebooks
resource "databricks_notebook" "notebook_model_serving" {
  source   = var.notebook_paths["model_serving"]
  path     = "/Shared/notebooks/09_genai/model_serving"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_rag" {
  source   = var.notebook_paths["rag"]
  path     = "/Shared/notebooks/09_genai/rag_pipeline"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_ai_agent" {
  source   = var.notebook_paths["ai_agent"]
  path     = "/Shared/notebooks/09_genai/ai_sales_agent"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_llm_checker" {
  source   = var.notebook_paths["llm_checker"]
  path     = "/Shared/notebooks/09_genai/llm_dq_checker"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_finetuning" {
  source   = var.notebook_paths["finetuning"]
  path     = "/Shared/notebooks/09_genai/finetuning"
  language = "PYTHON"
}

resource "databricks_notebook" "notebook_governance" {
  source   = var.notebook_paths["governance"]
  path     = "/Shared/notebooks/09_genai/governance"
  language = "PYTHON"
}

# GenAI Pipeline Job
resource "databricks_job" "genai_pipeline" {
  name = "GenAI-Pipeline-Mosaic-AI"

  task {
    task_key            = "model_serving_deploy"
    existing_cluster_id = databricks_cluster.genai.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_model_serving.path
      base_parameters = {
        "action" = "deploy"
      }
    }
  }

  task {
    task_key            = "rag_pipeline"
    existing_cluster_id = databricks_cluster.genai.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_rag.path
      base_parameters = {
        "action" = "build_vector_index"
      }
    }
    depends_on {
      task_key = "model_serving_deploy"
    }
  }

  task {
    task_key            = "ai_agent_build"
    existing_cluster_id = databricks_cluster.genai.id
    notebook_task {
      notebook_path = databricks_notebook.notebook_ai_agent.path
      base_parameters = {
        "action" = "build"
      }
    }
    depends_on {
      task_key = "rag_pipeline"
    }
  }

  email_notifications {
    on_failure = ["imjaykc31@gmail.com"]
    on_success = ["imjaykc31@gmail.com"]
  }
}
