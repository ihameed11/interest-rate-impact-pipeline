"""
Step 2a: prepare the MapReduce inputs from the raw SEC files.

For every quarter in blob storage (raw/sec_statements/<quarter>.zip):
  1. Read the zip from blob storage (or the local copy, if already downloaded).
  2. Copy its big "num" table (every reported number) into a gzip file,
     data/staging/num/num_<quarter>.txt.gz. Hadoop reads .gz files directly.
  3. Keep the header line of that table: its column order changed over the
     years, so the mapper needs to know the layout of each quarter.
  4. From the small "sub" table (one row per filing), keep only annual
     reports (10-K, 10-K/A), with the company's ID, name, sector code,
     country, fiscal year and filing date.

Outputs (in data/staging/):
  num/num_<quarter>.txt.gz   the numbers, one file per quarter
  num_headers.tsv            quarter -> column names of its num table
  filings_10k.tsv            every annual report filed 2020-2026

Usage (from the project folder, with .venv active):
  python3 -m src.mapreduce.prepare_inputs
"""
import csv
import gzip
import io
import shutil
import zipfile

from src.common import blob, config

SUB_COLUMNS = ["adsh", "cik", "name", "sic", "countryba", "stprba",
               "form", "period", "fy", "fp", "filed"]


def local_zip(quarter: str):
    """Path of the quarter's zip on this machine; downloaded from blob if missing."""
    path = config.LOCAL_RAW_DIR / "sec_statements" / f"{quarter}.zip"
    if not path.exists():
        print(f"  {quarter}: downloading from blob storage")
        blob.download_file(config.RAW_CONTAINER, f"sec_statements/{quarter}.zip", path)
    return path


def main():
    staging = config.LOCAL_STAGING_DIR
    (staging / "num").mkdir(parents=True, exist_ok=True)

    quarters = sorted(
        name.split("/")[-1].removesuffix(".zip")
        for name, _ in blob.list_blobs(config.RAW_CONTAINER, "sec_statements/")
        if name.endswith(".zip")
    )
    print(f"Preparing {len(quarters)} quarters: {quarters[0]} to {quarters[-1]}")

    headers, filings = [], []
    for quarter in quarters:
        with zipfile.ZipFile(local_zip(quarter)) as z:
            # --- num table: copy as-is into a .gz file, remember its header
            out = staging / "num" / f"num_{quarter}.txt.gz"
            with z.open("num.txt") as src:
                header = src.readline().decode("utf-8").rstrip("\n")
                headers.append((quarter, header))
                if not out.exists():
                    with gzip.open(out, "wb", compresslevel=5) as dst:
                        dst.write((header + "\n").encode("utf-8"))
                        shutil.copyfileobj(src, dst, length=4 * 1024 * 1024)

            # --- sub table: keep annual reports only
            with z.open("sub.txt") as src:
                reader = csv.DictReader(
                    io.TextIOWrapper(src, encoding="utf-8", errors="replace"),
                    delimiter="\t", quoting=csv.QUOTE_NONE,
                )
                kept = 0
                for row in reader:
                    if row["form"] in config.ANNUAL_FORMS:
                        filings.append([(row.get(c) or "").replace("\t", " ") for c in SUB_COLUMNS])
                        kept += 1
        print(f"  {quarter}: num table ready, {kept:,} annual reports")

    with open(staging / "num_headers.tsv", "w") as f:
        for quarter, header in headers:
            f.write(f"{quarter}\t{header}\n")

    with open(staging / "filings_10k.tsv", "w") as f:
        f.write("\t".join(SUB_COLUMNS) + "\n")
        for row in filings:
            f.write("\t".join(row) + "\n")

    # Keep a copy of the filings list in blob storage for the Spark step.
    blob.ensure_container(config.PROCESSED_CONTAINER)
    blob.upload_file(staging / "filings_10k.tsv", config.PROCESSED_CONTAINER,
                     "reference/filings_10k.tsv")
    print(f"Done: {len(filings):,} annual reports in total, inputs in {staging}")


if __name__ == "__main__":
    main()
