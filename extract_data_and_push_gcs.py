import requests
import csv
from google.cloud import storage
from google.oauth2 import service_account

# Set your Google Cloud Storage project ID
# project_id = 'bigquery-demo-415906'
# credentials = service_account.Credentials.from_service_account_file("C:/Users/Nirvik/Downloads/bigquery-demo-415906-3f7d02fa0cdb.json")

url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

querystring = {"formatType": "odi"}

headers = {
    "X-RapidAPI-Key": "0f5f2dd011msh8e6c76c34536ba3p18af25jsn6f65eeccfeff",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

if response.status_code == 200:
    data = response.json().get('rank', [])
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written as '{csv_filename}")

        # Upload the CSV file to GCS with project ID specified
        bucket_name = 'bat-ranking-data'
        # storage_client = storage.Client(project=project_id)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")

    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)
