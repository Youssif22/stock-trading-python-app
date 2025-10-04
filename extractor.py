from dotenv import load_dotenv
import os
import requests
import time
import csv
import pandas as pd
import snowflake.connector
from datetime import datetime


load_dotenv()  # Load environment variables from .env file
API_KEY = os.getenv("POLYGON_API_KEY")


LIMIT = 1000

def fetch_tickers():
    date_of_fetch = datetime.now().strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    tickers = []
    if "results" in data:
        tickers.extend(data["results"])

    while "next_url" in data:
        print(f"Fetching {data['next_url']}")
        time.sleep(15)  # prevent hitting free-tier rate limits
        response = requests.get(data["next_url"] + f"&apiKey={API_KEY}")
        data = response.json()

        if "results" not in data:
            print("Error or rate limit hit:", data)
            break

        tickers.extend(data["results"])

    # Example structure for CSV header
    example_ticker = {
        'ticker': 'ZYXI',
        'name': 'ZYNEX INC',
        'market': 'stocks',
        'locale': 'us',
        'primary_exchange': 'XNAS',
        'type': 'CS',
        'active': True,
        'currency_name': 'usd',
        'cik': '0000846475',
        'composite_figi': 'BBG000BJBXZ2',
        'share_class_figi': 'BBG001S7T7V0',
        'last_updated_utc': '2025-10-03T06:05:45.892037241Z',
        'date_of_fetch': date_of_fetch
    }
    cols = list(example_ticker.keys()) #columns for inserting into snowflake table


    rows = []
    for t in tickers:
        rows.append([t.get(c) if c != 'date_of_fetch' else date_of_fetch for c in cols])


    table_name = os.getenv("SNOWFLAKE_TABLE", "stock_tickers")

    #connect to snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(rows)} rows into {table_name} in Snowflake.")
    return tickers

if __name__ == "__main__":
    fetch_tickers()
