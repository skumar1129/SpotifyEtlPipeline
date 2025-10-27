# GCP-Native ETL Pipeline for Spotify Data

This project provides the complete infrastructure-as-code (IaC) and application code for a scalable, resilient ETL pipeline built entirely on Google Cloud Platform's managed services. The pipeline ingests data from multiple Spotify API endpoints, processes and normalizes it using Spark, models it in BigQuery using dbt, and validates the final datasets with Great Expectations.

## Architecture Overview

The pipeline is orchestrated by **Cloud Composer (Airflow)** and follows a modern ELT (Extract, Load, Transform) pattern.

1. **Extract (Airflow)**: Python tasks within an Airflow DAG call three simulated Spotify API endpoints (e.g., recently-played, artists, tracks).

    - **Incremental Loading**: The recently-played endpoint is loaded incrementally using a timestamp watermark managed by Airflow.

    - **Rate Limiting**: A custom Python function with exponential backoff and retries handles Spotify's API rate limits.

    - **Scaling**: Ingestion tasks can be parallelized (e.g., separate tasks for tracks and artists). For massive-scale ingestion, this could be extended by fanning out to Cloud Functions or managing a pool of API keys in Secret Manager.

2. **Load (Airflow -> GCS)**: Raw JSON data from the APIs is loaded directly into a **Google Cloud Storage (GCS)** "landing" bucket. This decouples the extraction from the transformation.

3. **Transform (Spark on Dataproc)**:

    - An Airflow task submits a PySpark job to a **Dataproc Serverless** (or ephemeral cluster).

    - The Spark job reads the raw JSON from the landing bucket, flattens the nested structures, normalizes the schemas, and merges data from the different sources.

    - The processed, clean data is written in Parquet format to a "processing" bucket in GCS.

4. **Model (dbt -> BigQuery)**:

    - Airflow triggers a `dbt run` command.

    - dbt connects to **BigQuery** and runs a series of SQL-based models against the Parquet files in the processing bucket (which are exposed as external tables) or loads them into native BigQuery tables.

    - This step builds the final, partitioned analytics tables (e.g., `fct_daily_listens`, `dim_tracks`) that will store the 1.2M+ daily records. Tables are partitioned by day (`played_at_day`) for cost and performance.

5. **Validate (Great Expectations)**:

    - After the dbt run, Airflow triggers a Great Expectations checkpoint.

    - This checkpoint runs a suite of expectations (e.g., "row count > 1,200,000", "primary key is unique", "timestamp is recent") against the final BigQuery tables.

6. **Alerting**:

    - The Airflow DAG is configured with an `on_failure_callback` that integrates with Google Cloud Monitoring (or a service like Slack/PagerDuty) to send alerts if any task fails, ensuring >99.95% uptime can be monitored.

### Architecture Diagram (Mermaid)

```mermaid
graph TD
    subgraph "Orchestration (Cloud Composer)"
        A[DAG: Schedule @daily] --> B(Extract: recently-played);
        A --> C(Extract: artists);
        A --> D(Extract: tracks);
    end

    subgraph "Ingestion (Python Operators)"
        B -- JSON --> E[GCS Landing Bucket];
        C -- JSON --> E;
        D -- JSON --> E;
    end

    subgraph "Processing (Dataproc)"
        E -- Triggers --> F[Spark Job: Normalize & Merge];
        F -- Parquet --> G[GCS Processing Bucket];
    end

    subgraph "Data Warehouse (BigQuery)"
        G -- External Table --> H[dbt: Staging Models];
        H --> I[dbt: Mart Models (Fact/Dim)];
        I -- Partitioned Tables --> J[BigQuery Analytics Dataset];
    end

    subgraph "Validation & Alerting"
        J -- Run Checkpoint --> K[Great Expectations];
        K -- Success/Failure --> L[Alerting: Cloud Monitoring / Slack];
    end

    %% Define Task Dependencies
    B --> F;
    C --> F;
    D --> F;
    F --> H;
    I --> K;
```


### Repository Organization

```
.
├── README.md                 # This file
├── infrastructure/           # Terraform IaC for all GCP resources
│   ├── main.tf
│   ├── variables.tf
│   ├── composer.tf
│   ├── storage.tf
│   ├── bigquery.tf
│   ├── dataproc.tf
│   └── iam.tf
├── airflow_dag/              # Python file to be deployed to Cloud Composer
│   └── spotify_etl_dag.py
├── spark_jobs/               # PySpark job for Dataproc
│   └── normalize_and_merge.py
├── dbt_project/              # dbt project for BigQuery transformations
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       │   └── stg_spotify__recently_played.sql
│       └── marts/
│           └── fct_daily_listens.sql
└── great_expectations/       # GE project (simplified)
    ├── expectations/
    │   └── fct_daily_listens_suite.json
    └── run_validation.py     # Helper script to run checkpoint

```

### Setup and Deployment

1. **Prerequisites**:

    - A Google Cloud Project with billing enabled.

    - The `gcloud` CLI, Terraform, and dbt installed locally.

    - Spotify Developer App credentials (Client ID, Client Secret).

2. **Provision Infrastructure (Terraform)**:

    - Navigate to the `infrastructure/` directory.

    - Create a `terraform.tfvars` file with your `project_id`, `region`, etc.

    - Run `terraform init`, `terraform plan`, and `terraform apply`.

    - This will create your Composer environment, GCS buckets, BigQuery dataset, and IAM service account.

3. **Configure Secrets**:

    - Store your Spotify Client ID and Secret in **GCP Secret Manager**.

    - Grant your Composer environment's service account permission to access these secrets.

    - In the Airflow UI, create Airflow Variables for `gcp_project`, `gcs_landing_bucket`, `gcs_processing_bucket`, etc.

4. **Deploy Pipeline Artifacts**:

    - **Airflow DAG**: Copy `airflow_dag/spotify_etl_dag.py` to your Composer environment's `dags/` folder (usually a GCS bucket).

    - **Spark Job**: Copy `spark_jobs/normalize_and_merge.py` to a GCS bucket (e.g., `gs://<your-project>-artifacts/spark-jobs/`).

    - **dbt Project**: Upload your `dbt_project/` folder to a GCS bucket or check it into a Git repository that Composer can access (e.g., via `GitSync`).

    - **Great Expectations**: Upload the `great_expectations/` project to your Composer environment's GCS bucket so it can be accessed by the DAG.

5. **Run the Pipeline**:

    - Access the Airflow UI for your Composer environment.

    - Un-pause the `spotify_gcp_etl` DAG and trigger a manual run.

### Key Assumptions

- **Services**: This architecture prioritizes fully managed services (Composer, Dataproc Serverless, BigQuery) to minimize operational overhead.

- **Spotify APIs**: We simulate three APIs. The `recently-played` endpoint is the only one treated as incremental. `artists` and `tracks` are treated as dimension lookups.

- **dbt Execution**: The DAG runs dbt using a `BashOperator`. For a more robust setup, you would use a KubernetesPodOperator, the `dbt-airflow` provider, or dbt Cloud.

- **GE Execution**: Validation is run via calling a python script. The `GreatExpectationsOperator` could also be used.

- **Dataproc**: The Terraform file sets up a Dataproc cluster controller (a template). The Airflow DAG then submits a job to this template, which provisions a serverless batch or an ephemeral cluster, runs the job, and tears it down. This is highly cost-effective.