import http.client
import json
import os
import csv
from google.cloud import storage

# ------------------ CONFIG ------------------
API_KEY = "0715f903a8mshceee232221f9c06p1261f7jsncd979828ce6a"
GCS_BUCKET_NAME = "cricket_stat_demo"  # Change this if needed
GCS_DESTINATION_BLOB = "test_batting_rankings.csv"

# Save CSV locally
csv_path = os.path.expanduser("C:/Users/user/Documents/test_batting_rankings.csv")
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
# --------------------------------------------

# Step 1: Make the API call
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
headers = {
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com",
    'x-rapidapi-key': API_KEY
}
conn.request("GET", "/stats/v1/rankings/batsmen?formatType=test", headers=headers)
res = conn.getresponse()
data = res.read()

# Step 2: Parse JSON
try:
    ranking_data = json.loads(data.decode("utf-8"))
    players = ranking_data.get("rank", [])
except json.JSONDecodeError as e:
    print(" JSON parsing failed:", e)
    players = []

# Step 3: Save as CSV locally (without Rank)
if players:
    with open(csv_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["Name", "Country", "Rating"])
        writer.writeheader()
        for player in players:
            writer.writerow({
                "Name": player.get("name"),
                "Country": player.get("country"),
                "Rating": player.get("rating")
            })
    print(f" CSV saved at: {csv_path}")
else:
    print(" No player data found. Skipping CSV creation.")

# Step 4: Upload CSV to GCS
def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f" Uploaded to GCS bucket '{bucket_name}' as '{destination_blob_name}'")
    except Exception as e:
        print(" GCS upload failed:", e)

# Only upload if CSV was created
if os.path.exists(csv_path) and players:
    upload_to_gcs(csv_path, GCS_BUCKET_NAME, GCS_DESTINATION_BLOB)
