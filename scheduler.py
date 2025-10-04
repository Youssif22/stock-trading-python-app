import schedule
import time
from datetime import datetime
import script
from extractor import fetch_tickers


def job():
    print(f"[{datetime.now().isoformat()}] Starting daily fetch…")
    fetch_tickers()
    print(f"[{datetime.now().isoformat()}] Done.")

if __name__ == "__main__":
    import schedule
    schedule.every().day.at("09:00").do(job)  # <-- set your local time HH:MM (24h)

    print("Scheduler running. Next run at 09:00 daily.")
    while True:
        schedule.run_pending()
        time.sleep(30)