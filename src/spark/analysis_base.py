"""
Shared starting point for the research-question jobs.

Loads the company_ratios table from blob storage and keeps the companies
where the ratios make sense:
  - not banks/insurers (debt is their raw material, not a burden)
  - not shell companies (SPACs: no real business)
  - with positive total assets
  - ESTABLISHED companies only: at least $100 million revenue in 2021.
    Without this filter the medians are dominated by thousands of small,
    loss-making firms (e.g. early-stage biotech) whose operating income is
    negative whatever the interest rate, which hides the effect we study.
    Zombie-firm studies use a similar rule (they exclude young firms).
"""
from pyspark.sql import functions as F

from src.spark.session import blob

MIN_REVENUE_2021 = 100_000_000  # US dollars


def load_operating_companies(spark, established_only=True):
    df = (spark.read.parquet(blob("processed", "company_ratios"))
          .filter("NOT is_financial AND NOT is_shell AND total_assets > 0"))
    if established_only:
        established = (df.filter(f"fy = 2021 AND revenue >= {MIN_REVENUE_2021}")
                         .select("cik").distinct())
        df = df.join(established, "cik")
    return df


def median(col):
    return F.expr(f"percentile_approx({col}, 0.5)")
