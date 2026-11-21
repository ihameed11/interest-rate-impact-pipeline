"""
Shared settings for every Python step of the pipeline.

Private values (keys, emails, passwords) come from the .env file in the
project folder, which is never uploaded to GitHub. Everything else that
might change (which years, which quarters) is defined here, in one place.
"""
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# Project root = two folders up from this file (src/common/config.py).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def require(name: str) -> str:
    """Read a setting from .env and stop with a clear message if it is missing."""
    value = os.environ.get(name, "").strip()
    if not value or value.startswith(("your_", "paste_", "Your Name")):
        raise SystemExit(
            f"Setting {name} is missing or still a placeholder in your .env file. "
            f"See .env.example for what to put there."
        )
    return value


# ---------- Study period ----------
# Fiscal years we analyse. 2020-2021 = low interest rates,
# 2022 = rates rising, 2023-2025 = high interest rates.
FIRST_FISCAL_YEAR = 2020
LAST_FISCAL_YEAR = 2025

# A company's annual report (10-K) for fiscal year Y is filed during year Y+1,
# so we need SEC files from the start of 2020 to well into 2026.
FIRST_FILING_QUARTER = (2020, 1)
LAST_FILING_QUARTER = (2026, 3)


def filing_quarters():
    """All quarters to download, as strings like '2020q1'."""
    year, q = FIRST_FILING_QUARTER
    quarters = []
    while (year, q) <= LAST_FILING_QUARTER:
        quarters.append(f"{year}q{q}")
        q += 1
        if q == 5:
            year, q = year + 1, 1
    return quarters


# ---------- Local folders (ignored by Git) ----------
LOCAL_RAW_DIR = PROJECT_ROOT / "data" / "raw"          # copies of what is in blob "raw"
LOCAL_STAGING_DIR = PROJECT_ROOT / "data" / "staging"  # inputs/outputs of MapReduce

# The ~23 SEC tags we keep (one financial line item can have several tags).
LINE_ITEMS_FILE = PROJECT_ROOT / "src" / "common" / "line_items.csv"

# Annual reports only (original and amended).
ANNUAL_FORMS = {"10-K", "10-K/A"}

# ---------- Azure Blob Storage ----------
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "processed"

# ---------- Web sources ----------
SEC_STATEMENTS_URL = (
    "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{quarter}.zip"
)
SEC_COMPANIES_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NYFED_EFFR_URL = "https://markets.newyorkfed.org/api/rates/unsecured/effr/search.json"

# Interest-rate history from one year before the study period, up to today.
RATES_START = date(FIRST_FISCAL_YEAR - 1, 1, 1)
RATES_END = date.today()
