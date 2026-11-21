"""
Runs all of Step 1 (data collection) in order, then lists what is in the
"raw" container so you can see the result.

Usage (from the project folder):
  python3 -m src.ingest.run_all                 # everything
  python3 -m src.ingest.run_all 2023q1 2024q1   # only these SEC quarters (for testing)
"""
import sys

from src.common import blob, config
from src.ingest import download_sec_statements, fetch_interest_rates, fetch_sec_companies


def main():
    quarters = sys.argv[1:] or config.filing_quarters()

    # Make sure both containers exist, so a fresh storage account also works.
    blob.ensure_container(config.RAW_CONTAINER)
    blob.ensure_container(config.PROCESSED_CONTAINER)

    print("=== 1a. SEC financial statements ===")
    download_sec_statements.main(quarters)
    print("\n=== 1b. SEC company list ===")
    fetch_sec_companies.main()
    print("\n=== 1c. Interest rates ===")
    fetch_interest_rates.main()

    print(f"\n=== Contents of the '{config.RAW_CONTAINER}' container ===")
    total = 0
    for name, size in blob.list_blobs(config.RAW_CONTAINER):
        total += size
        print(f"  {name:<55} {size / 1_000_000:>8.1f} MB")
    print(f"  {'TOTAL':<55} {total / 1_000_000:>8.1f} MB")


if __name__ == "__main__":
    main()
