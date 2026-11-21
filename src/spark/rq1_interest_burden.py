"""
RQ1: Did the interest burden rise most for the companies that carried the
most debt BEFORE rates went up?

Method
  1. Take each company's debt level in 2021 (the last low-rate year):
     debt_to_assets = total liabilities / total assets.
  2. Split companies into three equal groups: low / medium / high debt.
  3. For every year, compare the groups on:
       - median cost of debt        (interest paid / long-term debt)
       - median interest coverage   (operating income / interest paid)
       - share of companies whose operating income does not cover
         their interest bill (coverage below 1)

Writes: rq1_interest_burden  (PostgreSQL + blob)
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.spark.analysis_base import load_operating_companies, median
from src.spark.session import get_spark, write_table


def main():
    spark = get_spark("rq1-interest-burden")
    df = load_operating_companies(spark)

    # Debt group fixed on 2021, so the groups do not change over time.
    baseline = (df.filter("fy = 2021 AND debt_to_assets IS NOT NULL")
                  .withColumn("tercile", F.ntile(3).over(Window.orderBy("debt_to_assets")))
                  .select("cik", F.element_at(F.array(F.lit("low debt"), F.lit("medium debt"),
                                                      F.lit("high debt")), F.col("tercile"))
                                 .alias("debt_group_2021")))

    result = (df.join(baseline, "cik")
                .filter("interest_expense > 0")
                .groupBy("fy", "avg_policy_rate", "rate_period", "debt_group_2021")
                .agg(F.count("*").alias("companies"),
                     F.round(median("cost_of_debt") * 100, 2).alias("median_cost_of_debt_pct"),
                     F.round(median("interest_coverage"), 2).alias("median_interest_coverage"),
                     F.round(F.avg(F.when(F.col("interest_coverage") < 1, 1).otherwise(0)) * 100, 1)
                      .alias("pct_coverage_below_1"))
                .orderBy("debt_group_2021", "fy"))

    result.show(30, truncate=False)
    write_table(result, "rq1_interest_burden")
    spark.stop()


if __name__ == "__main__":
    main()
