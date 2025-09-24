from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.utils.dates import days_ago
from datetime import timedelta

# -------------------------------
# CONFIG
# -------------------------------
PROJECT_ID = "sound-harbor-470812-q6"
REGION = "asia-south1"
CLUSTER_NAME = "demo-dp"

BUCKET = "rabbani_proc"
RAW_PATH = f"gs://{BUCKET}/raw"
CURATED_PATH = f"gs://{BUCKET}/curated"
PYSPARK_FILE = f"gs://{BUCKET}/scripts/data_proc_job.py"

BQ_DATASET = "retail_analytics"

tables = {
    "dim_customers": "curated/dim_customers/*.parquet",
    "dim_products": "curated/dim_products/*.parquet",
    "fact_sales": "curated/fact_sales/*",  # partitioned
    "customer_360": "curated/customer_360/*.parquet",
}

# -------------------------------
# Default Args
# -------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------
# Tasks using Hooks
# -------------------------------

@task
def submit_dataproc_job():
    """Run PySpark job on Dataproc using hook."""
    hook = DataprocHook(gcp_conn_id="google_cloud_default")
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_FILE,
            "args": ["--raw", RAW_PATH, "--out", CURATED_PATH],
        },
    }
    hook.submit_job(project_id=PROJECT_ID, region=REGION, job=job)


@task
def create_bq_dataset():
    """Create BigQuery dataset using hook (idempotent)."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION

    try:
        client.create_dataset(dataset, exists_ok=True)  # idempotent
        print(f"Dataset {BQ_DATASET} created or already exists.")
    except Exception as e:
        raise RuntimeError(f"Failed to create dataset: {e}")


@task
def load_parquet_to_bq(table, source_obj):
    """Load parquet files from GCS to BigQuery using hook."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()

    uri = f"gs://{BUCKET}/{source_obj}"
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job completion
    print(f"Table {table} loaded successfully from {uri}.")


# -------------------------------
# DAG definition
# -------------------------------
with DAG(
    "retail_etl_dataproc_bq_hooks_partitioned",
    default_args=default_args,
    description="ETL pipeline (hooks, partitioned): GCS raw → Dataproc → GCS curated → BigQuery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["gcp", "dataproc", "bq", "hooks", "partitioned"],
) as dag:

    run_dataproc = submit_dataproc_job()
    create_dataset_task = create_bq_dataset()

    load_tasks = [
        load_parquet_to_bq.override(task_id=f"load_{table}")(table, source_obj)
        for table, source_obj in tables.items()
    ]

    # Set dependencies
    run_dataproc >> create_dataset_task >> load_tasks
