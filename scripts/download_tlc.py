import os
import boto3
from pathlib import Path
from urllib.request import urlretrieve
import tempfile

bucket = os.environ["S3_BUCKET"]
raw_prefix = os.environ.get("RAW_PREFIX", "raw")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = ["2025-01", "2025-02", "2025-03"]
# Use platform-agnostic temp directory
LOCAL_DIR = Path(tempfile.gettempdir()) / "tlc"

def download():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    for month in MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        url = f"{BASE_URL}/{filename}"
        local_path = LOCAL_DIR / filename
        print(f"Downloading {url} -> {local_path}")
        urlretrieve(url, local_path)

def upload():
    s3 = boto3.client("s3")
    for month in MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        local_path = LOCAL_DIR / filename
        year, mon = month.split("-")
        s3_key = f"{raw_prefix}/tlc/yellow/{year}/{mon}/{filename}"
        print(f"Uploading {local_path} -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(local_path), bucket, s3_key)

if __name__ == "__main__":
    download()
    upload()

