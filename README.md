# 📊 Crypto Price ETL Pipeline

A lightweight ETL (Extract–Transform–Load) pipeline that Extracts live cryptocurrency market data from the CoinGecko API, transforms it into a clean structure, and loads it into a CSV file inside dedicated folders for data and logs.

---

### 🚀 Features

- Extracts top 10 cryptocurrencies by market cap

- Cleans & transforms price records

- Adds timestamp for data collection

- Saves to /data/crypto_data.csv

### 🛠 Tech Stack

- Python
- Requests (API calls)
- Pandas (transformation)
