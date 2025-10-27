"""
Airflow DAG to Extract, Load, and Transform Spotify Data on GCP
"""

import datetime
import json
import logging
import time
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import requests
from requests.adapters import HTTPAdapter, Retry

# --- GCP & Project Variables ---
# These should be set in Airflow Variables
GCP_PROJECT_ID = Variable.get("gcp_project_id", "a-gcp-project-id")
GCP_REGION = Variable.get("gcp_region", "us-central1")
GCS_LANDING_BUCKET = Variable.get("gcs_landing_bucket", "a-landing-bucket")
GCS_PROCESSING_BUCKET = Variable.get("gcs_processing_bucket", "a-processing-bucket")
GCS_ARTIFACT_BUCKET = Variable.get("gcs_artifacts_bucket", "a-artifacts-bucket")
BQ_DATASET = Variable.get("bq_dataset_name", "spotify_data")
DATAPROC_CLUSTER_NAME = Variable.get("dataproc_cluster_name", "ephemeral-spark-cluster")

# --- Spotify API Variables ---
# Stored in GCP Secret Manager, accessed via env vars in Composer
SPOTIFY_CLIENT_ID_SECRET = Variable.get("SPOTIFY_CLIENT_ID_SECRET", "")
SPOTIFY_CLIENT_SECRET_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET_SECRET", "")
SPOTIFY_BASE_URL = "https://api.spotify.com/v1"

# --- Other Configs ---
DBT_PROJECT_DIR = "/home/airflow/gcs/dags/dbt_project" # Assumes GitSync
GE_PROJECT_DIR = "/home/airflow/gcs/dags/great_expectations" # Assumes GitSync

# --- Helper Functions ---
def get_spotify_token():
    """Retrieves an OAuth token from Spotify."""
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    
    client_id = client.access_secret_version(name=SPOTIFY_CLIENT_ID_SECRET).payload.data.decode("UTF-8")
    client_secret = client.access_secret_version(name=SPOTIFY_CLIENT_SECRET_SECRET).payload.data.decode("UTF-8")

    auth_response = requests.post(
        "https://accounts.spotify.com/api/token",
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    auth_response.raise_for_status()
    return auth_response.json()["access_token"]


def create_retry_session():
    """Creates a requests session with exponential backoff."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,  # 2s, 4s, 8s, 16s, 32s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def send_alert(context):
    """Sends a failure alert. Placeholder for Cloud Monitoring or Slack."""
    logging.error(f"Task {context['task_instance_key_str']} failed.")
    # TODO: Add integration with Slack, PagerDuty, or GCP Monitoring
    pass


# --- DAG Definition ---
@dag(
    dag_id="spotify_gcp_etl",
    start_date=datetime.datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["gcp", "spotify", "etl", "spark", "dbt", "ge"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": send_alert,
    },
)
def spotify_gcp_etl():
    """
    Orchestrates the Spotify ETL pipeline on GCP.
    """
    
    execution_date_str = "{{ ds }}"
    execution_date_dt = "{{ data_interval_start }}"

    @task
    def get_auth_token():
        """Fetches the Spotify API token and passes it via XCom."""
        return get_spotify_token()

    @task_group(group_id="extract_and_load")
    def extract_and_load(api_token: str):
        """
        Task group to extract data from Spotify APIs and load to GCS.
        """

        @task
        def extract_recently_played(token: str, data_interval_start: datetime.datetime):
            """
            Extracts incrementally. Gets data played *after* the start of the DAG run interval.
            """
            session = create_retry_session()
            headers = {"Authorization": f"Bearer {token}"}
            
            # Convert to Unix millisecond timestamp for Spotify API
            after_timestamp = int(data_interval_start.timestamp() * 1000)
            
            url = f"{SPOTIFY_BASE_URL}/me/player/recently-played"
            params = {"limit": 50, "after": after_timestamp}
            
            all_plays = []
            while url:
                try:
                    response = session.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    all_plays.extend(data["items"])
                    
                    # Check for pagination
                    if "next" in data and data["next"]:
                        url = data["next"]
                        params = {} # Next URL already includes params
                    else:
                        url = None
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        retry_after = int(e.response.headers.get("Retry-After", 60))
                        logging.warning(f"Rate limited. Waiting for {retry_after} seconds.")
                        time.sleep(retry_after)
                    else:
                        raise e
            
            logging.info(f"Extracted {len(all_plays)} recently played tracks.")
            return all_plays

        @task
        def extract_artist_data(token: str):
            """
            Simulates extracting artist data. In prod, this would be fed by
            artist_ids from the 'recently_played' task.
            """
            # 'recently_played' task and deduplicate them.
            artist_ids = ["0TnOYISbd1XYRBk9myBCg0", "1vCWHaC5f2uS3yhpwWbIA6"] # Example: Post Malone, The Weeknd
            
            session = create_retry_session()
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{SPOTIFY_BASE_URL}/artists"
            params = {"ids": ",".join(artist_ids)}
            
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            logging.info("Extracted artist data.")
            return response.json().get("artists", [])

        # (Simulate a third API call for tracks)
        @task
        def extract_track_data(token: str):
            """Simulates extracting track data."""
            track_ids = ["0e7ipj03S05BNilyu5bRzt", "1XgE2VjOdeCaXgCq1fHASA"]
            
            session = create_retry_session()
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{SPOTIFY_BASE_URL}/tracks"
            params = {"ids": ",".join(track_ids)}
            
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            logging.info("Extracted track data.")
            return response.json().get("tracks", [])

        @task
        def load_to_gcs(data: list, object_name: str, exec_date: str):
            """Saves the extracted data as a newline-delimited JSON file in GCS."""
            if not data:
                logging.warning(f"No data to load for {object_name}.")
                return None

            # Convert list of dicts to newline-delimited JSON string
            ndjson_data = "\n".join(json.dumps(item) for item in data)
            
            gcs_path = f"raw/{object_name}/date={exec_date}/{object_name}.jsonl"
            
            # This task is a placeholder for GCSUploadStringOperator or similar
            # We'll use a PythonOperator to "upload"
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
            gcs_hook = GCSHook()
            gcs_hook.upload(
                bucket_name=GCS_LANDING_BUCKET,
                object_name=gcs_path,
                data=ndjson_data,
                mime_type="application/octet-stream",
            )
            logging.info(f"Loaded {object_name} data to gs://{GCS_LANDING_BUCKET}/{gcs_path}")
            return f"gs://{GCS_LANDING_BUCKET}/{gcs_path}"

        # --- Define Task Flow for this Group ---
        plays = extract_recently_played(token=api_token, data_interval_start=execution_date_dt)
        artists = extract_artist_data(token=api_token)
        tracks = extract_track_data(token=api_token)

        load_to_gcs(plays, "recently_played", execution_date_str)
        load_to_gcs(artists, "artists", execution_date_str)
        load_to_gcs(tracks, "tracks", execution_date_str)

    # Spark Transformation Job
    submit_spark_job = DataprocSubmitPySparkJobOperator(
        task_id="submit_spark_transform",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        main=f"gs://{GCS_ARTIFACT_BUCKET}/spark_jobs/normalize_and_merge.py",
        cluster_name=DATAPROC_CLUSTER_NAME, # Assumes an ephemeral cluster config
        dataproc_jars=["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar"],
        job_name="spotify_normalize_merge_{{ ds_nodash }}",
        # Pass arguments to the PySpark script
        pyfiles=[], # Add any zip dependencies here
        args=[
            f"--input_plays_path=gs://{GCS_LANDING_BUCKET}/raw/recently_played/date={execution_date_str}/*.jsonl",
            f"--input_artists_path=gs://{GCS_LANDING_BUCKET}/raw/artists/date={execution_date_str}/*.jsonl",
            f"--input_tracks_path=gs://{GCS_LANDING_BUCKET}/raw/tracks/date={execution_date_str}/*.jsonl",
            f"--output_path=gs://{GCS_PROCESSING_BUCKET}/processed/date={execution_date_str}/",
        ],
    )

    # Expose the processed data to BigQuery as an external table
    # This allows dbt to read directly from GCS Parquet files
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_for_dbt",
        bucket=GCS_PROCESSING_BUCKET,
        source_objects=[f"processed/date={execution_date_str}/*.parquet"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.stg_external_spotify_listens",
        source_format="PARQUET",
        autodetect=True, # Let BigQuery infer the schema from Parquet
        # This table is temporary, just for dbt to read
        # dbt will materialize its own tables
    )

    @task_group(group_id="run_dbt")
    def run_dbt():
        """Task group to run dbt commands."""
        
        # Runs dbt models
        dbt_run = BashOperator(
            task_id="dbt_run",
            # Assumes dbt_project.yml and profiles.yml are set up in the dir
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --target prod",
            # env={"DBT_BIGQUERY_KEYFILE": "/path/to/sa-key.json"} # If using keyfile
        )

        # Runs dbt tests
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --target prod",
        )
        
        dbt_run >> dbt_test

    @task(task_id="run_great_expectations_validation")
    def run_ge_validation():
        """
        Runs a Great Expectations checkpoint against the final BigQuery table.
        """
        # This calls a script, which is more flexible than the operator
        # Requires 'great_expectations' to be installed in the Composer env
        
        # We need to install the script's dependencies if they aren't in the
        # main Composer environment. A better way is using KubernetesPodOperator.
        # For simplicity, we use BashOperator.
        
        # This assumes the GE project is available in the dags folder
        # and configured to connect to BigQuery.
        validation_command = f"""
        python {GE_PROJECT_DIR}/run_validation.py \
            --checkpoint_name "fct_daily_listens.chk" \
            --ge_project_root "{GE_PROJECT_DIR}"
        """
        
        # Using BashOperator to run the python script
        run_validation = BashOperator(
            task_id="ge_checkpoint_bash",
            bash_command=validation_command,
        )
        run_validation.execute(context={}) # This is simplified
        

    # --- Define Full DAG Dependencies ---
    auth_token = get_auth_token()
    extract_load_group = extract_and_load(api_token=auth_token)
    
    extract_load_group >> submit_spark_job
    submit_spark_job >> create_external_table
    create_external_table >> run_dbt()
    run_dbt() >> run_ge_validation()


# Instantiate the DAG
spotify_gcp_etl()
