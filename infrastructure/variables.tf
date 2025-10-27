variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for resources."
  type        = string
  default     = "us-central1"
}

variable "environment_name" {
  description = "A name for the environment (e.g., 'dev', 'prod')."
  type        = string
  default     = "dev"
}

variable "composer_env_name" {
  description = "Name for the Cloud Composer environment."
  type        = string
  default     = "spotify-etl-composer"
}

variable "bq_dataset_name" {
  description = "Name for the BigQuery dataset."
  type        = string
  default     = "spotify_data"
}
