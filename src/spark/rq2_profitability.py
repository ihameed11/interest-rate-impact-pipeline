"""
RQ2: Which sectors lost (or gained) profitability between the low-rate
years (2019-2021) and the high-rate years (2023-2025)?

For each sector and period: median net margin, ROA and ROE, then the
change from the low-rate to the high-rate period, in percentage points.

Writes: rq2_sector_profitability  (PostgreSQL + blob)
"""
from pyspark.sql import functions as F

from src.spark.analysis_base import load_operating_companies, median
from src.spark.session import get_spark, write_table


def main():
    spark = get_spark("rq2-profitability")
    df = load_operating_companies(spark).filter("rate_period IN ('low', 'high')")

    by_period = (df.groupBy("sector", "rate_period")
                   .agg(F.countDistinct("cik").alias("companies"),
                        (median("net_margin") * 100).alias("net_margin_pct"),
                        (median("roa") * 100).alias("roa_pct"),
                        (median("roe") * 100).alias("roe_pct")))

    low = by_period.filter("rate_period = 'low'").drop("rate_period")
    high = by_period.filter("rate_period = 'high'").drop("rate_period")
    cols = ["companies", "net_margin_pct", "roa_pct", "roe_pct"]
    result = (low.select("sector", *[F.col(c).alias(f"{c}_low") for c in cols])
              .join(high.select("sector", *[F.col(c).alias(f"{c}_high") for c in cols]), "sector")
              .withColumn("net_margin_change_pp", F.col("net_margin_pct_high") - F.col("net_margin_pct_low"))
              .withColumn("roa_change_pp", F.col("roa_pct_high") - F.col("roa_pct_low"))
              .withColumn("roe_change_pp", F.col("roe_pct_high") - F.col("roe_pct_low")))
    result = result.select("sector", *[F.round(c, 2).alias(c) if c != "sector" and "companies" not in c
                                      else F.col(c) for c in result.columns if c != "sector"])
    result = result.orderBy("roa_change_pp")

    result.show(truncate=False)
    write_table(result, "rq2_sector_profitability")
    spark.stop()


if __name__ == "__main__":
    main()
