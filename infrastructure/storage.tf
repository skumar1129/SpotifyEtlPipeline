#
# Google Cloud Storage
#

# Bucket for landing raw JSON data
resource "google_storage_bucket" "landing_bucket" {
  name          = "${local.gcs_prefix}-landing-zone"
  location      = "US"
  force_destroy = true # OK for dev, remove for prod
  
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # Delete raw data after 30 days
    }
  }
}

# Bucket for processed Parquet data
resource "google_storage_bucket" "processing_bucket" {
  name          = "${local.gcs_prefix}-processing-zone"
  location      = "US"
  force_destroy = true
}

# Bucket for pipeline artifacts (Spark jobs, dbt models, GE suites)
resource "google_storage_bucket" "artifacts_bucket" {
  name          = "${local.gcs_prefix}-artifacts"
  location      = "US"
  force_destroy = true
}
