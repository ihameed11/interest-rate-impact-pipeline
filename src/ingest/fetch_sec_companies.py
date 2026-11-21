"""
Step 1b: fetch the SEC's list of listed companies (JSON API).

One call returns every company with its CIK (the SEC's company ID, which
links to the financial statement files), name, ticker and stock exchange.
The answer is stored unchanged in the "raw" container under sec_companies/,
with today's date in the file name so earlier versions are kept.

Usage (from the project folder):
  python3 -m src.ingest.fetch_sec_companies
"""
import json
from datetime import date

import requests

from src.common import blob, config


def main():
    headers = {"User-Agent": config.require("SEC_USER_AGENT")}
    print(f"Calling {config.SEC_COMPANIES_URL}")
    response = requests.get(config.SEC_COMPANIES_URL, headers=headers, timeout=60)
    response.raise_for_status()
    payload = response.json()

    # Quick check so we notice if the SEC changes the format.
    print(f"  Fields: {payload['fields']}")
    print(f"  Companies: {len(payload['data']):,}")

    blob_name = f"sec_companies/company_tickers_exchange_{date.today():%Y%m%d}.json"
    blob.upload_bytes(json.dumps(payload).encode(), config.RAW_CONTAINER, blob_name)
    print(f"  Stored as {config.RAW_CONTAINER}/{blob_name}")


if __name__ == "__main__":
    main()
