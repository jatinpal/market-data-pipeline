import yfinance as yf
import json
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from google.cloud import storage

def load_tickers(in_path):
    try:
        with open(Path(in_path), 'r', encoding="utf-8") as f:
            data = json.load(f)
            return [item['symbol'] for item in data["tickers"]]
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

def upload_to_gcs(local_file, bucket_name, gcs_path):
    """Upload a file to Google Cloud Storage"""
    try:
        client = storage.Client(project='market-data-pipeline-alpha')
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file)
        print(f"  Uploaded to gs://{bucket_name}/{gcs_path}")
        return True
    except Exception as e:
        print(f"  GCS Upload Error: {e}")
        return False

def save_data(out_path, tickers, business_date, upload_gcs=False, bucket_name=None):
    directory = Path(f"{out_path}/{business_date}")
    directory.mkdir(parents=True, exist_ok=True)

    start_date = datetime.strptime(business_date, "%Y-%m-%d")
    next_day = start_date + timedelta(days=1)
    next_day = next_day.strftime("%Y-%m-%d")

    for ticker in tickers:
        try:
            print(f"Fetching {ticker}...")
            t_data = yf.Ticker(ticker)
            hist = t_data.history(start=start_date, end=next_day)
            
            if hist.empty:
                print(f"Warning: No data for {ticker}")
                continue
            
            # Save locally
            save_file = Path(f"{directory}/{ticker}.json")
            hist.reset_index().to_json(save_file, orient='records', date_format='iso', lines=True)
            print(f"Success: {ticker}")
            
            # Upload to GCS if enabled
            if upload_gcs and bucket_name:
                gcs_path = f"raw/price/{business_date}/{ticker}.json"
                upload_to_gcs(save_file, bucket_name, gcs_path)
            
        except Exception as e:
            print(f"Failure: {ticker} Error: {e}")
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract market data")
    parser.add_argument("--path_tickers", default="./tickers.json", help="Path for the ticker list")
    parser.add_argument("--path_extract", default="./data/raw/", help="Path for extracted raw data")
    parser.add_argument("--business_date", default=datetime.now().strftime("%Y-%m-%d"), help="Business date for data extract")
    parser.add_argument("--upload_gcs", action="store_true", help="Upload to Google Cloud Storage")
    parser.add_argument("--gcs_bucket", default="market-data-lake-alpha", help="GCS bucket name")

    args = parser.parse_args()
    
    print(f"Loading data for tickers from {args.path_tickers}...")
    tickers = load_tickers(args.path_tickers)
    print(f"Loaded data for {len(tickers)} tickers")

    print(f"Saving ticker data for {args.business_date}...")
    save_data(args.path_extract, tickers, args.business_date, args.upload_gcs, args.gcs_bucket)
    print(f"Saved ticker data for {args.business_date}")