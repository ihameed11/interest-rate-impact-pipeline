"""
Shared starting point for the research-question jobs.

Loads the company_ratios table from blob storage and keeps the companies
where debt ratios make sense:
  - not banks/insurers (debt is their raw material, not a burden)
  - not shell companies (SPACs: no real business)
  - with positive total assets
"""
from pyspark.sql import functions as F

from src.spark.session import blob


def load_operating_companies(spark):
    return (spark.read.parquet(blob("processed", "company_ratios"))
            .filter("NOT is_financial AND NOT is_shell AND total_assets > 0"))


def median(col):
    return F.expr(f"percentile_approx({col}, 0.5)")
