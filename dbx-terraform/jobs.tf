resource "databricks_job" "airbnb_dbt_job" {
  name = "New Job Oct 06, 2025, 11:24 AM"

  task {
    task_key = "airbnb-dbt"

    dbt_task {
      project_directory = "dbtlearn"
      commands          = ["dbt run --select src dim_listings_cleansed"]
      schema            = "airbnb"
      warehouse_id      = "498abaf462b01385"
      catalog           = "workspace"
      source            = "GIT"
    }

    environment_key = "dbt-default"
  }

  git_source {
    url      = "https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero/"
    provider = "gitHub"
    branch   = "databricks"
  }

  queue {
    enabled = true
  }

  environment {
    environment_key = "dbt-default"
    
    spec {
      client = "3"
      
      dependencies = [
        "dbt-databricks>=1.0.0,<2.0.0"
      ]
    }
  }

  performance_target = "PERFORMANCE_OPTIMIZED"
}
