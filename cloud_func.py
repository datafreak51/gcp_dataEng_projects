import functions_framework
from googleapiclient.discovery import build
import logging

@functions_framework.cloud_event
def trigger_df_job(cloud_event):
    logging.basicConfig(level=logging.INFO)

    data = cloud_event.data
    bucket = data["bucket"]
    file_name = data["name"]
    gcs_file_path = f"gs://{bucket}/{file_name}"

    project = "youtube-trending-analysis-demo"
    region = "us-central1"
    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    service = build('dataflow', 'v1b3')

    job_name = "trigger-csv-gcs-job02"

    template_body = {
        "jobName": job_name,
        "parameters": {
            "inputFilePattern": gcs_file_path,
            "javascriptTextTransformGcsPath": "gs://cricket_stat_demo/udf.js",
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "youtube-trending-analysis-demo.demo_dataflow_set.test_batting_rankings",
            "JSONPath": "gs://cricket_stat_demo/schema/stat_schema.json",  # ✅ Correct key
            #"fileFormat": "CSV",
            #"skipLeadingRows": "1",  # ✅ Skip CSV header
            "bigQueryLoadingTemporaryDirectory": "gs://customer_databuck/temp/"
        },
        "environment": {
            "tempLocation": "gs://tempraroybucket/temp"
        }
    }

    try:
        request = service.projects().templates().launch(
            projectId=project,
            gcsPath=template_path,
            location=region,
            body=template_body
        )
        response = request.execute()
        logging.info(f"Dataflow job triggered successfully: {response}")
    except Exception as e:
        logging.error(f"Failed to trigger Dataflow job: {e}")
