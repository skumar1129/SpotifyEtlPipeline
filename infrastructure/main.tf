terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  project_id = var.project_id
  region     = var.region
  env_name   = var.environment_name
  gcs_prefix = "${var.project_id}-${var.environment_name}"

  service_account_email = "sa-spotify-etl@${var.project_id}.iam.gserviceaccount.com"
}
