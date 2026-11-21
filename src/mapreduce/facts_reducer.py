#!/usr/bin/env python3
"""
MapReduce step 2, REDUCE side.

Hadoop sorts the mapper output, so all values for one key
(company | fiscal year | tag) arrive here next to each other.

A company may report the same number more than once, for example in the
original 10-K and again in a corrected 10-K/A. We keep the value from the
LATEST filing, so corrections replace the original figure.

Output (tab-separated): cik, fiscal_year, tag, value, filing_id, filed_date
"""
import sys


def flush(key, best):
    if key is not None:
        cik, fy, tag = key.split("|")
        filed, adsh, value = best
        print(f"{cik}\t{fy}\t{tag}\t{value}\t{adsh}\t{filed}")


def main():
    current_key, best = None, None
    groups = duplicates = 0
    for line in sys.stdin:
        key, value = line.rstrip("\n").split("\t", 1)
        filed, adsh, number = value.split("|")
        if key != current_key:
            flush(current_key, best)
            current_key, best = key, (filed, adsh, number)
            groups += 1
        else:
            duplicates += 1
            if filed > best[0]:  # dates are YYYYMMDD, so text order = date order
                best = (filed, adsh, number)
    flush(current_key, best)
    sys.stderr.write(f"reporter:counter:SEC,company_year_items,{groups}\n")
    sys.stderr.write(f"reporter:counter:SEC,duplicates_resolved,{duplicates}\n")


if __name__ == "__main__":
    main()
