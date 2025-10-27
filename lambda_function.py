import requests
import csv
import json
import boto3
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET = 'tuf-narayan-data-bucket'

def fetch_and_store(data_format, source_url, s3_path):
    """
    Downloads data from a given URL, converts it to JSON, and uploads to S3.
    
    :param data_format: Expected format of the data ('json' or 'csv')
    :param source_url: URL to fetch the data from
    :param s3_path: Path (key) to store the data in S3
    """
    res = requests.get(source_url)
    res.raise_for_status()

    if data_format == 'json':
        content = res.json()
    elif data_format == 'csv':
        reader = csv.DictReader(StringIO(res.text))
        content = list(reader)
    else:
        raise ValueError("Unsupported format. Choose 'json' or 'csv'.")

    s3.put_object(
        Bucket=BUCKET,
        Key=f"{s3_path}.json",
        Body=json.dumps(content)
    )
    print(f"Data uploaded to {s3_path}.json")

def lambda_handler(event, context):
    """
    AWS Lambda handler to process multiple datasets.
    """
    sources = [
        {"format": "json", "url": "https://api.tfl.gov.uk/Line/Mode/tube/Status", "path": "tube_status"},
        {"format": "csv", "url": "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv", "path": "store_products"}
    ]

    for source in sources:
        fetch_and_store(source["format"], source["url"], source["path"])

    return {
        "status": "completed",
        "details": "All sources processed and uploaded to S3."
    }