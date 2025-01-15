import requests
import boto3
import json
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
S3_BUCKET_NAME = "default-s3-bucket-mf"
S3_FILE_KEY = (
    "mf_list_output/mutual_fund_list.json"  # S3 path where the file will be stored
)


def fetch_mutual_fund_json(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def save_json_file(json_data, file_name):
    with open(file_name, "w") as file:
        json.dump(json_data, file)


def upload_to_s3(local_file_path):
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(local_file_path, S3_BUCKET_NAME, S3_FILE_KEY)
        print(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")
    except FileNotFoundError:
        print("Error: The file was not found.")
    except NoCredentialsError:
        print("Error: AWS credentials not available.")


def lambda_handler(event, context):
    # Fetch data from API
    url = "https://api.mfapi.in/mf"
    json_data = fetch_mutual_fund_json(url)

    # Save data to JSON file
    local_json_path = "/tmp/mutual_fund_list.json"
    if json_data:
        save_json_file(json_data, local_json_path)
        # Upload the JSON file to S3
        upload_to_s3(local_json_path)

    return {"statusCode": 200, "body": json.dumps("Data successfully uploaded to S3")}
