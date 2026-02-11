import os
import time
import argparse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from tqdm import tqdm
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

CREDENTIALS_FILE = "de-hw3-key.json"
BASE_URL_PATTERN = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{type}/{type}_tripdata_{year}-{month}.csv.gz"
)


def download(url, dest_dir, retries=3, wait=5):
    filename = os.path.basename(url)
    dest_path = os.path.join(dest_dir, filename)
    if os.path.exists(dest_path):
        return dest_path

    for attempt in range(1, retries + 1):
        try:
            urllib.request.urlretrieve(url, dest_path)
            return dest_path
        except Exception:
            if attempt == retries:
                raise
            time.sleep(wait)


def upload_to_gcs(file_path, bucket_name, credentials=CREDENTIALS_FILE, max_retries=3):
    client = None
    if credentials:
        client = storage.Client.from_service_account_json(credentials)
    else:
        client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)

    for attempt in range(1, max_retries + 1):
        try:
            blob.upload_from_filename(file_path)
            return f"gs://{bucket_name}/{blob_name}"
        except Exception:
            if attempt == max_retries:
                raise
            time.sleep(5)


def generate_urls(years, months, types):
    for taxi_type, year, month in product(types, years, months):
        yield BASE_URL_PATTERN.format(type=taxi_type, year=year, month=f"{month:02d}")


def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC data files")
    parser.add_argument("--years", default="2019,2020", help="Comma-separated years")
    parser.add_argument("--months", default="1-12", help="Single month or range (e.g. 1,3,5 or 1-12)")
    parser.add_argument("--types", default="yellow,green", help="Comma-separated taxi types")
    parser.add_argument("--dest", default="data", help="Destination directory")
    parser.add_argument("--workers", type=int, default=6, help="Parallel downloads")
    parser.add_argument("--gcs-bucket", default="datazoomcamp-hw3-bucket", help="GCS bucket name to upload files")
    parser.add_argument("--gcs-creds", default=None, help="Path to GCS service account JSON (optional)")
    parser.add_argument("--upload", action="store_true", help="Upload downloaded files to GCS")
    args = parser.parse_args()

    years = [int(y) for y in args.years.split(",")]

    # parse months (accept ranges)
    if "-" in args.months:
        start, end = args.months.split("-")
        months = list(range(int(start), int(end) + 1))
    else:
        months = [int(m) for m in args.months.split(",")]

    types = [t.strip() for t in args.types.split(",")]

    os.makedirs(args.dest, exist_ok=True)

    urls = list(generate_urls(years, months, types))

    downloaded_files = []
    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = {exe.submit(download, url, args.dest): url for url in urls}
        for fut in tqdm(as_completed(futures), total=len(futures)):
            url = futures[fut]
            try:
                path = fut.result()
                if path:
                    downloaded_files.append(path)
                    tqdm.write(f"Downloaded: {path}")
            except Exception as e:
                tqdm.write(f"Failed: {url} -> {e}")

    if args.upload:
        # upload concurrently
        with ThreadPoolExecutor(max_workers=min(8, max(1, args.workers))) as exe:
            upload_futs = {exe.submit(upload_to_gcs, fp, args.gcs_bucket, args.gcs_creds): fp for fp in downloaded_files}
            for fut in tqdm(as_completed(upload_futs), total=len(upload_futs)):
                src = upload_futs[fut]
                try:
                    gcs_path = fut.result()
                    tqdm.write(f"Uploaded {src} -> {gcs_path}")
                except Exception as e:
                    tqdm.write(f"Upload failed {src} -> {e}")


if __name__ == "__main__":
    main()
