import argparse
import  os
import urllib.request

def download(url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True) #create the directory if doesn't exists
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"Already exists, skipping: {dest_path}")
        return
    print(f"downloading:\n {url}\n-> {dest_path}")
    urllib.request.urlretrieve(url, dest_path)
    print("Done.")

def main(ingest_month, format):
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--month", required=True, help="YYYY-MM (e.g., 2024-01")
    # parser.add_argument("--format", choices=["parquet","csv"], default="parquet")
    # args = parser.parse_args()

    # yyyy, mm = args.month.split("-")
    yyyy, mm = ingest_month.split("-")
    # TLC filenames follow patterns like:
    # yellow_tripdata_2024-01.parquet (or .csv)
    # filename = f"yellow_tripdata_{yyyy}-{mm}.{args.format}"
    filename = f"yellow_tripdata_{yyyy}-{mm}.{format}"

    # TLC hosts monthly files at a known location; this generally works for modern datasets.
    # If a specific month is not available in parquet, use --format csv.
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f"{base_url}/{filename}"

    out_dir = f"/home/jovyan/project/data/raw/nyc_taxi/yellow/ingest_month={ingest_month}"
    dest_path = os.path.join(out_dir, filename)

    download(url, dest_path)

if __name__ == "__main__":
    ## download data for 2023-2025
    format = 'parquet'
    for year in range(2023, 2026):
        for month in range(1,13):
            month = ('0' + str(month))[-2:]
            year = str(year)
            ingest_month = f"{year}-{month}"
            main(ingest_month, format)
    # main()