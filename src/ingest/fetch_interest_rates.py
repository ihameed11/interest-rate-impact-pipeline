"""
Step 1c: fetch daily US interest rates from the New York Fed (JSON API).

We use the Effective Federal Funds Rate (EFFR): the main US policy interest
rate, published every business day. Later, Spark turns it into a yearly
average to separate the low-rate years from the high-rate years.

The answer is stored unchanged in the "raw" container under rates/.

Usage (from the project folder):
  python3 -m src.ingest.fetch_interest_rates
"""
import json

import requests

from src.common import blob, config


def main():
    params = {
        "startDate": config.RATES_START.isoformat(),
        "endDate": config.RATES_END.isoformat(),
    }
    print(f"Calling {config.NYFED_EFFR_URL} for {params['startDate']} to {params['endDate']}")
    response = requests.get(config.NYFED_EFFR_URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()

    rates = payload["refRates"]
    first, last = min(r["effectiveDate"] for r in rates), max(r["effectiveDate"] for r in rates)
    print(f"  Daily rates received: {len(rates):,} ({first} to {last})")

    blob_name = "rates/effr_daily.json"
    blob.upload_bytes(json.dumps(payload).encode(), config.RAW_CONTAINER, blob_name)
    print(f"  Stored as {config.RAW_CONTAINER}/{blob_name}")


if __name__ == "__main__":
    main()
