import yfinance as yf
import json
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from google.cloud import storage
import pandas as pd

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
    
def fetch_and_combine_data(tickers, business_date):
    """Fetch data for all tickers and combine into single DataFrame"""
    start_date = datetime.strptime(business_date, "%Y-%m-%d")
    next_day = start_date + timedelta(days=1)
    next_day_str = next_day.strftime("%Y-%m-%d")
    
    all_data = []
    
    for ticker in tickers:
        try:
            print(f"Fetching {ticker}...")
            t_data = yf.Ticker(ticker)
            hist = t_data.history(start=start_date, end=next_day)
            
            if hist.empty:
                print(f"Warning: No data for {ticker}")
                continue
            
            # Add ticker column and reset index
            hist = hist.reset_index()
            hist['Ticker'] = ticker
            
            all_data.append(hist)
            print(f"Success: {ticker}")
            
        except Exception as e:
            print(f"Failure: {ticker} Error: {e}")
            continue
    
    if not all_data:
        print("Error: No data fetched for any ticker")
        return None
    
    # Combine all DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Standardize column names (replace spaces with underscores)
    combined_df.columns = combined_df.columns.str.replace(' ', '_')
    
    # Fill missing columns with None/null
    # This handles cases where some tickers have columns others don't
    
    return combined_df

def save_data(out_path, tickers, business_date, upload_gcs=False, bucket_name=None):
    """Fetch data and save as single JSON file"""
    combined_df = fetch_and_combine_data(tickers, business_date)

    if combined_df is None:
        return

    directory = Path(f"{out_path}")
    directory.mkdir(parents=True, exist_ok=True)

     # Save as single file: YYYY-MM-DD.json
    save_file = directory / f"{business_date}.json"
    combined_df.to_json(save_file, orient='records', date_format='iso', lines=True)
    print(f"\nSaved combined data: {save_file}")
    print(f"Total rows: {len(combined_df)}")
    
    # Upload to GCS if enabled
    if upload_gcs and bucket_name:
        gcs_path = f"raw/price/{business_date}.json"
        upload_to_gcs(save_file, bucket_name, gcs_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract market data")
    parser.add_argument("--path_tickers", default="./tickers.json", help="Path for the ticker list")
    parser.add_argument("--path_extract", default="./data/raw/", help="Path for extracted raw data")
    parser.add_argument("--business_date", default=datetime.now().strftime("%Y-%m-%d"), help="Business date for data extract")
    parser.add_argument("--upload_gcs", action="store_true", help="Upload to Google Cloud Storage")
    parser.add_argument("--gcs_bucket", default="market-data-lake-alpha", help="GCS bucket name")

    args = parser.parse_args()
    
    print(f"Loading tickers from {args.path_tickers}...")
    tickers = load_tickers(args.path_tickers)
    print(f"Loaded {len(tickers)} tickers")

    print(f"Saving ticker data for {args.business_date}...")
    save_data(args.path_extract, tickers, args.business_date, args.upload_gcs, args.gcs_bucket)
    print(f"Saved ticker data for {args.business_date}")