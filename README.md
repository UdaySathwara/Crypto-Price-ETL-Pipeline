# 📊 Crypto Price ETL Pipeline

A simple and efficient ETL pipeline that extracts real-time cryptocurrency market data from the CoinGecko API, transforms it into a clean tabular structure, and loads the output into a CSV file.
This project includes automated folder creation and logging for pipeline monitoring.

---

### 🚀 Features

- Extracts top 10 cryptocurrencies by market cap
- Cleans & transforms price records
- Includes a readable timestamp for each ETL run
- Saves the output to:
      - data/crypto_data.csv
- Logs all ETL activity to:
      - logs/crypto_etl.log

---

### 🛠 Tech Stack

- Python
- Requests (API calls)
- Pandas (transformation)

---

### 📂 Repository Structure

```

Crypto-Price-ETL-Pipeline
│── main.py
│── data/
│   └── crypto_data.csv     # auto-created after first run that store data 
│── logs/
│   └── crypto_etl.log      # auto-created
|
│__ readme.md               # Project overview and instructions

```
---

## 🌟 About the Author

**Uday Sathwara** — B.Tech Computer Engineering student. Passionate about data engineering, building ETL pipelines, and creating analytics solutions.
