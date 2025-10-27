#
# BigQuery Dataset and Tables
#

resource "google_bigquery_dataset" "spotify_dataset" {
  dataset_id = var.bq_dataset_name
  location   = "US"
  description = "Dataset for Spotify analytics data"
}

# This table will be created and managed by dbt
# We define it here to show partitioning
resource "google_bigquery_table" "fct_daily_listens" {
  dataset_id = google_bigquery_dataset.spotify_dataset.dataset_id
  table_id   = "fct_daily_listens"
  
  # Partition by day for 1.2M daily records
  time_partitioning {
    type = "DAY"
    field = "played_at_day" # This column must exist in the table
  }

  # Cluster on common query dimensions
  clustering = ["user_id", "artist_id"]

  schema = <<EOF
[
  {"name": "listen_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "track_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "artist_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "played_at_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "played_at_day", "type": "DATE", "mode": "NULLABLE"}
]
EOF

  # This table will be populated by dbt, so we'll delete it if terraform is destroyed
  deletion_protection = false
}
