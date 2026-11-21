#!/usr/bin/env python3
"""
MapReduce step 2, MAP side (runs on every Hadoop worker, in parallel).

Input:  lines of the SEC "num" table (one reported number per line).
Output: only the numbers we need, as   key \t value
        key   = cik | fiscal year | SEC tag
        value = filing date | filing ID | number

A line is kept only if ALL of these hold:
  - it comes from an annual report (10-K or 10-K/A)        -> map-side join
  - its tag is one of our ~23 tags (src/common/line_items.csv)
  - it is in US dollars, for the whole company (no segment / subsidiary)
  - it is a full-year amount (qtrs=4) or a year-end balance (qtrs=0)
  - it is for the report's own year, not a prior-year comparison

Side files (sent to every worker by Hadoop with -files):
  filings_10k.tsv   annual reports: filing ID -> company, fiscal year, period
  num_headers.tsv   column layout of the num table for each quarter
  line_items.csv    the SEC tags we keep
"""
import csv
import os
import re
import sys


def load_filings():
    filings = {}
    with open("filings_10k.tsv") as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split("\t")
            adsh, cik, period, fy, filed = p[0], p[1], p[7], p[8], p[10]
            if fy:
                filings[adsh] = (cik, fy, period, filed)
    return filings


def load_headers():
    headers = {}
    with open("num_headers.tsv") as f:
        for line in f:
            quarter, header = line.rstrip("\n").split("\t", 1)
            headers[quarter] = header.split("\t")
    return headers


def main():
    filings = load_filings()
    with open("line_items.csv") as f:
        tags = {row["tag"] for row in csv.DictReader(f)}

    # Which quarter's file is this mapper reading? Hadoop tells us.
    input_file = os.environ.get("mapreduce_map_input_file") or os.environ.get("map_input_file", "")
    match = re.search(r"num_(\d{4}q\d)", input_file)
    columns = load_headers()[match.group(1)]
    col = {name: i for i, name in enumerate(columns)}
    i_adsh, i_tag, i_ddate = col["adsh"], col["tag"], col["ddate"]
    i_qtrs, i_uom, i_value = col["qtrs"], col["uom"], col["value"]
    i_coreg, i_segments = col.get("coreg"), col.get("segments")

    read = kept = 0
    for line in sys.stdin:
        read += 1
        p = line.rstrip("\n").split("\t")
        if len(p) < len(columns) - 1 or p[i_adsh] == "adsh":
            continue  # broken line or the header line
        filing = filings.get(p[i_adsh])
        if (filing is None or p[i_tag] not in tags or p[i_uom] != "USD"
                or p[i_qtrs] not in ("0", "4") or not p[i_value]):
            continue
        if i_coreg is not None and p[i_coreg]:
            continue  # a subsidiary, not the whole company
        if i_segments is not None and i_segments < len(p) and p[i_segments]:
            continue  # a business segment, not the whole company
        cik, fy, period, filed = filing
        if p[i_ddate] != period:
            continue  # prior-year comparison figure
        kept += 1
        print(f"{cik}|{fy}|{p[i_tag]}\t{filed}|{p[i_adsh]}|{p[i_value]}")

    # Counters show up in the Hadoop job summary (rows read vs. rows kept).
    sys.stderr.write(f"reporter:counter:SEC,num_rows_read,{read}\n")
    sys.stderr.write(f"reporter:counter:SEC,num_rows_kept,{kept}\n")


if __name__ == "__main__":
    main()
