"""
Crypto Price ETL Pipeline
-------------------------
Extracts live cryptocurrency market data from the CoinGecko API,
transforms it into a clean structure, and loads it into a CSV file
inside dedicated folders for data and logs.

Features:
- Extracts top 10 cryptocurrencies by market cap
- Adds timestamp for data collection
- Saves to /data/crypto_data.csv
"""

import requests
import pandas as pd
from datetime import datetime
import time
import logging
import os

# Folder Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create folders if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Configure Logging
log_file = os.path.join(LOG_DIR, "crypto_etl.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Extract
def extract_data():
    """Extracts top 10 cryptocurrencies by market cap from CoinGecko API."""
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 10,
            "page": 1,
            "sparkline": "false"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info("Data extraction successful.")
        return data
    except Exception as e:
        logging.error(f"Error in data extraction: {e}")
        raise

# Transform
def transform_data(data):
    """Transforms raw data into a clean DataFrame."""
    try:
        df = pd.DataFrame(data, columns=[
            "id", "symbol", "current_price", "market_cap", "total_volume"
        ])

        df.rename(columns={
            "id": "crypto_name",
            "symbol": "symbol",
            "current_price": "price_usd",
            "market_cap": "market_cap_usd",
            "total_volume": "volume_usd"
        }, inplace=True)

        # Add readable timestamp
        df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logging.info("Data transformation successful.")
        return df

    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        raise

# Load
def load_data(df):
    """Loads the DataFrame into a CSV file inside the /data folder."""
    try:
        output_file = os.path.join(DATA_DIR, "crypto_data.csv")
        df.to_csv(output_file, index=False)
        logging.info(f"Data loading successful. Data saved to {output_file}")
        print(f"Data successfully saved to {output_file}")
    except Exception as e:
        logging.error(f"Error in data loading: {e}")
        raise

# ETL Pipeline
def etl_process():
    """Main ETL process."""
    start_time = time.time()
    logging.info("ETL process started.")

    # Step 1: Extract
    data = extract_data()

    # Step 2: Transform
    df = transform_data(data)

    # Step 3: Load
    load_data(df)

    end_time = time.time()
    logging.info(f"ETL process completed in {end_time - start_time:.2f} seconds.")

# Run ETL process

if __name__ == "__main__":
    etl_process()

# # Run the ETL process every 1 minutes
# while True:
#     etl_process()
#     time.sleep(60)  # Sleep for 1 minute