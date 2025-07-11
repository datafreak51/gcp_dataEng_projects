Project Summary: **Cricket Stats ETL**
  Source:
     Cricbuzz API via RapidAPI
  Steps:
    1. Extract:
      * Used `http.client` to call the Cricbuzz API (batsmen rankings in test format).
      * Retrieved data as JSON format.
    2. Transform:
      * Parsed the JSON.
      * Extracted `name`, `country`, `rating`.
      * Saved data as CSV.
    3. Load:
     * Uploaded the CSV to **Google Cloud Storage (GCS)**.
     * Once csv file is available in gcs bucket it will trigger cloud run function, cloud run function will trigger                dataflow job which move data from gcs to bq using in built template Text from cloud storage to Big query 



 Technologies Used:

* Python (for scripting)
* GCS (for storage)
* Git (for version control)
* Airflow (you wrote a DAG to trigger the script)
* VS Code (your dev environment)\
* Cloud run function
* Dataflow
* BigQuery

