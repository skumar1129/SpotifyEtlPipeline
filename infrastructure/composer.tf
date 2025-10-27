#
# Cloud Composer 2 Environment
#
resource "google_composer_environment" "spotify_etl_env" {
  name   = var.composer_env_name
  region = var.region

  config {
    software_config {
      image_version = "composer-2-airflow-2" # Use a modern version
      pypi_packages = {
        "apache-airflow-providers-google" = ""
        "dbt-bigquery"                    = ""
        "great-expectations"              = ""
        "requests"                        = ""
      }
      # Store secrets in Secret Manager, not Airflow Vars
      env_variables = {
        "SPOTIFY_CLIENT_ID_SECRET" = "projects/${var.project_id}/secrets/spotify-client-id/versions/latest"
        "SPOTIFY_CLIENT_SECRET_SECRET" = "projects/${var.project_id}/secrets/spotify-client-secret/versions/latest"
      }
    }
    
    # Use a cost-effective machine type for dev
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
      }
    }
    
    # Use the dedicated service account on the VM nodes
    node_config {
      service_account = google_service_account.pipeline_sa.email
    }
  }
}
