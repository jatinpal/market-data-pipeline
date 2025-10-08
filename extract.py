import yfinance as yf
import json
from pathlib import Path
from datetime import datetime
import argparse

def load_tickers(in_path):
    
    try:
        with open(Path(in_path), 'r', encoding="utf-8") as f:
            data = json.load(f)
            return [item['symbol'] for item in data["tickers"]]

    except Exception as e:
        print(f"Error: {e}")
        exit(1)

def save_data(out_path, tickers, business_date):

    directory = Path(f"{out_path}/{business_date}")
    directory.mkdir(parents=True, exist_ok=True)

    for ticker in tickers:
        try:
            print(f"Fetching {ticker}...")
            t_data = yf.Ticker(ticker)
            hist = t_data.history(period="1d")
            
            if hist.empty:
                print(f"Warning: No data for {ticker}")
                continue
            
            save_file = Path(f"{directory}/{ticker}.json")
            hist.to_json(save_file, orient='records', date_format='iso', indent=2)
            print(f"Success: {ticker}")
            
        except Exception as e:
            print(f"Failure: {ticker} Error: {e}")
            continue

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Extract market data")
    parser.add_argument("--path_tickers", default="./tickers.json", help="Path for the ticker list")
    parser.add_argument("--path_extract", default="./data/raw/", help="Path for extracted raw data")
    parser.add_argument("--business_date", default=datetime.now().strftime("%Y-%m-%d"), help="Business date for data extract")

    args = parser.parse_args()

    ticker_arg = args.path_tickers
    extract_arg = args.path_extract
    date_arg = args.business_date
    
    print(f"Loading data for tickers from {ticker_arg} ...")
    tickers = load_tickers(ticker_arg)
    print(f"Loaded data for {len(tickers)} tickers")

    print(f"Saving ticker data for {date_arg} ...")
    save_data(extract_arg, tickers, date_arg)
    print(f"Saved ticker data for {date_arg} at {extract_arg}")