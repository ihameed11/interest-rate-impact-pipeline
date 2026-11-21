"""
RQ3: Did companies' liquidity and financial health change, how many became
"zombie" firms, and how do Ireland-based companies compare?

Zombie firm: operating income has not covered the interest bill
(interest coverage below 1) for two years in a row.

For every year, for all companies and for Ireland-based companies:
  - median current ratio and cash / assets      (liquidity)
  - share of companies in the Altman "distress" zone
  - share of zombie firms

Writes: rq3_financial_health  (PostgreSQL + blob)
        rq3_zombie_firms      (the companies flagged, for the follow-up analysis)
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.spark.analysis_base import load_operating_companies, median
from src.spark.session import get_spark, write_table


def main():
    spark = get_spark("rq3-financial-health")
    df = load_operating_companies(spark)

    # Zombie flag: this year AND the previous year below 1x interest coverage.
    w = Window.partitionBy("cik").orderBy("fy")
    df = (df.withColumn("below_1", (F.col("interest_coverage") < 1) & (F.col("interest_expense") > 0))
            .withColumn("prev_fy", F.lag("fy").over(w))
            .withColumn("prev_below_1", F.lag("below_1").over(w))
            .withColumn("is_zombie", F.col("below_1") & F.col("prev_below_1")
                                     & (F.col("prev_fy") == F.col("fy") - 1)))

    def summarise(frame, group_name):
        return (frame.groupBy("fy", "avg_policy_rate", "rate_period")
                .agg(F.count("*").alias("companies"),
                     F.round(median("current_ratio"), 2).alias("median_current_ratio"),
                     F.round(median("cash_to_assets") * 100, 1).alias("median_cash_to_assets_pct"),
                     F.round(F.avg(F.when(F.col("health_zone") == "distress", 1).otherwise(0)) * 100, 1)
                      .alias("pct_distress_zone"),
                     F.round(F.avg(F.when(F.col("is_zombie"), 1).otherwise(0)) * 100, 1)
                      .alias("pct_zombie"))
                .withColumn("group", F.lit(group_name)))

    result = (summarise(df, "all companies")
              .unionByName(summarise(df.filter("is_ireland"), "Ireland-based"))
              .orderBy("group", "fy"))
    result.show(30, truncate=False)
    write_table(result, "rq3_financial_health")

    zombies = (df.filter("is_zombie")
                 .select("cik", "name", "ticker", "sector", "country", "fy",
                         F.round("interest_coverage", 2).alias("interest_coverage"),
                         "interest_expense", "operating_income", "long_term_debt"))
    print(f"Zombie company-years: {zombies.count():,}")
    write_table(zombies, "rq3_zombie_firms")
    spark.stop()


if __name__ == "__main__":
    main()
