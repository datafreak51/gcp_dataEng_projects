
GCP DE Project - Manual (GCS + Dataproc + BigQuery)

Folder structure you should mirror in your own GCS bucket:
- gs://YOUR_BUCKET/raw/                     (upload all raw files here)
- gs://YOUR_BUCKET/scripts/dataproc_transform.py
- gs://YOUR_BUCKET/curated/                 (job output)

Files included locally (download and upload to your bucket):
- raw/customers.csv
- raw/products.csv
- raw/sales_2025_01.csv
- raw/store_lookup.csv
- raw/events.jsonl
- raw/bad_rows.csv (intentionally messy)
- raw/product_price_updates_2025_01.csv
- scripts/dataproc_transform.py

Step 0: Prereqs (once)
- gcloud init
- gcloud config set project YOUR_PROJECT_ID
- Enable APIs: Compute Engine, Dataproc, BigQuery, Cloud Storage
- Create a regional bucket: gsutil mb -l asia-south1 gs://YOUR_BUCKET

Upload files:
- gsutil -m rsync -r /mnt/data/gcp_de_project gs://YOUR_BUCKET/

Create Dataproc cluster (single-node for demo):
- gcloud dataproc clusters create demo-dp --region=asia-south1 --single-node --image-version=2.2-debian12

Submit PySpark job:
- gcloud dataproc jobs submit pyspark     --cluster=demo-dp --region=asia-south1     --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar     gs://YOUR_BUCKET/scripts/dataproc_transform.py     -- --raw gs://YOUR_BUCKET/raw --out gs://YOUR_BUCKET/curated

Inspect output in GCS:
- gsutil ls gs://YOUR_BUCKET/curated/

Load to BigQuery from Parquet (simplest way):
- bq --location=asia-south1 mk --dataset if not exists demo_curated
- bq load --autodetect --replace --source_format=PARQUET demo_curated.dim_customers gs://YOUR_BUCKET/curated/dim_customers/*
- bq load --autodetect --replace --source_format=PARQUET demo_curated.dim_products gs://YOUR_BUCKET/curated/dim_products/*
- bq load --autodetect --replace --source_format=PARQUET demo_curated.fact_sales gs://YOUR_BUCKET/curated/fact_sales/*
- bq load --autodetect --replace --source_format=PARQUET demo_curated.customer_360 gs://YOUR_BUCKET/curated/customer_360/*

Alternative: Write to BigQuery directly from Spark (using the loaded connector):
- spark.write.format("bigquery").option("table","demo_curated.dim_customers").save()

Cleanup:
- gcloud dataproc clusters delete demo-dp --region=asia-south1
- (optional) gsutil -m rm -r gs://YOUR_BUCKET/curated/

