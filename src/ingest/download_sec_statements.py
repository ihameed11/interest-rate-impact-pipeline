"""
Step 1a: download the SEC Financial Statement Data Sets.

The SEC publishes one zip file per quarter with every number from every
financial statement filed that quarter (four tables: sub, num, tag, pre).
This script downloads each quarter and stores it, unchanged, in the "raw"
container of Azure Blob Storage under sec_statements/.

It is safe to run again: quarters already in blob storage are skipped.

Usage (from the project folder):
  python3 -m src.ingest.download_sec_statements                 # all quarters
  python3 -m src.ingest.download_sec_statements 2023q1 2024q1   # only these
"""
import sys
import time

import requests

from src.common import blob, config


def download_quarter(quarter: str, session: requests.Session) -> None:
    blob_name = f"sec_statements/{quarter}.zip"
    if blob.exists(config.RAW_CONTAINER, blob_name):
        print(f"  {quarter}: already in blob storage, skipped")
        return

    url = config.SEC_STATEMENTS_URL.format(quarter=quarter)
    local_path = config.LOCAL_RAW_DIR / "sec_statements" / f"{quarter}.zip"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"  {quarter}: downloading {url}")
    with session.get(url, stream=True, timeout=120) as response:
        if response.status_code == 404:
            print(f"  {quarter}: not published by the SEC yet, skipped")
            return
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    size_mb = local_path.stat().st_size / 1_000_000
    print(f"  {quarter}: {size_mb:.1f} MB downloaded, uploading to blob storage")
    blob.upload_file(local_path, config.RAW_CONTAINER, blob_name)
    print(f"  {quarter}: stored as {config.RAW_CONTAINER}/{blob_name}")


def main(quarters):
    # The SEC requires every program to identify itself with a name and email,
    # and allows at most 10 requests per second.
    session = requests.Session()
    session.headers.update({"User-Agent": config.require("SEC_USER_AGENT")})

    print(f"Downloading {len(quarters)} quarter(s) of SEC financial statements")
    for quarter in quarters:
        download_quarter(quarter, session)
        time.sleep(1)  # stay far below the SEC's rate limit
    print("Done.")


if __name__ == "__main__":
    main(sys.argv[1:] or config.filing_quarters())
