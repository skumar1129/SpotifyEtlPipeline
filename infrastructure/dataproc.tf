#
# Dataproc Cluster (Serverless / Ephemeral Template)
#
# This defines a *template* for a cluster.
# The Airflow DAG will submit a job to this, which provisions, runs, and terminates.

resource "google_dataproc_cluster" "pyspark_cluster" {
  # For simplicity, we'll use DataprocSubmitPySparkJobOperator and pass the config.
  # This resource is more for a persistent cluster, which we don't want.
  
  # Let's define a Dataproc Workflow Template instead, which is better for this.
  
  provider = google-beta # Workflow templates are often in beta

  # Required name for the Dataproc cluster resource (used only as a template placeholder)
  name    = "spotify-spark-transform"
  
  project = var.project_id
  region  = var.region
  
}
  # This resource creates a Dataproc *Workflow Template*
  # The Airflow DAG will call this template.
resource "google_dataproc_workflow_template" "spark_transform_workflow" {
    provider = google-beta
    name   = "spotify-spark-transform-template"
    location = var.region
    
    placement {
      managed_cluster {
        cluster_name = "ephemeral-spark-cluster" # Dataproc will append a UUID
        config {
          gce_cluster_config {
            service_account = google_service_account.pipeline_sa.email
            zone            = "${var.region}-b"
          }
          master_config {
            num_instances   = 1
            machine_type = "n1-standard-4"
          }
          worker_config {
            num_instances   = 2
            machine_type = "n1-standard-4"
          }
          software_config {
            image_version = "2.1-debian11" # Spark 3.x
          }
        }
      }
    }
    
    jobs {
      step_id = "run_spark_job"
      pyspark_job {
        # We will pass this main_python_file_uri from Airflow
        main_python_file_uri = "gs://${local.gcs_prefix}-artifacts/spark_jobs/normalize_and_merge.py"
        # We will pass args from Airflow
      }
    }
}

