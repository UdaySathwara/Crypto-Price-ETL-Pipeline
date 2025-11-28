"""
Crypto Price ETL Pipeline (Simplified)
--------------------------------------
Extracts top 10 cryptocurrencies from the CoinGecko API,
transforms them into a clean table, and saves the output
to a CSV file inside /data folder.

ETL Steps:
1. Extract  → Get crypto data from API
2. Transform → Clean and format the data
3. Load → Save data into CSV
"""

import requests
import pandas as pd
from datetime import datetime
import os
import time

# Folder Setup

# Get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create a "data" folder inside the project
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)


# Extract Step
def extract_data():
    """
    Extracts top 10 cryptocurrencies by market cap from CoinGecko API.

    Returns:
        list: Raw JSON data from API containing crypto details.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # Stops if API request fails
    return response.json()


# Transform Step
def transform_data(data):
    """
    Converts raw API JSON data into a structured and clean DataFrame.

    Args:
        data (list): Raw crypto data from the API.

    Returns:
        pandas.DataFrame: Cleaned table ready for loading.
    """
    # Convert selected fields into a DataFrame
    df = pd.DataFrame(data, columns=[
        "id", "symbol", "current_price", "market_cap", "total_volume"
    ])

    # Rename columns for better readability
    df.rename(columns={
        "id": "crypto_name",
        "current_price": "price_usd",
        "market_cap": "market_cap_usd",
        "total_volume": "volume_usd"
    }, inplace=True)

    # Add timestamp for when the data was collected
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df

# Load Step
def load_data(df):
    """
    Saves the cleaned DataFrame to a CSV file inside the 'data' folder.

    Args:
        df (pandas.DataFrame): The cleaned dataset.
    """
    output_file = os.path.join(DATA_DIR, "crypto_data.csv")
    df.to_csv(output_file, index=False)
    print(f"Data saved to: {output_file}")


# ETL Pipeline Controller
def etl_process():
    """
    Runs the complete ETL process: Extract → Transform → Load.
    """
    print("Starting ETL process...")

    data = extract_data()
    df = transform_data(data)
    load_data(df)

    print("ETL process completed successfully!\n")


# Run ETL
if __name__ == "__main__":
    etl_process()

    # To automate the ETL every minute, uncomment below:
    # while True:
    #     etl_process()
    #     time.sleep(60)
