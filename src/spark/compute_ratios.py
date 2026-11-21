"""
Spark job 3b: compute the FINANCIAL RATIOS for every company-year.

Reads: processed/company_years/ (Parquet in blob storage, from job 3a)

Ratios (the ones financial analysts use every day):
  Profitability  net_margin       = net income / revenue
                 roa              = net income / total assets
                 roe              = net income / shareholders' equity
  Debt burden    interest_coverage= operating income / interest expense
                 cost_of_debt     = interest expense / long-term debt
                 debt_to_equity   = long-term debt / equity
                 debt_to_assets   = total liabilities / total assets
  Liquidity      current_ratio    = current assets / current liabilities
                 cash_to_assets   = cash / total assets
  Health         altman_z         = Altman Z''-score (bankruptcy-risk score)

A ratio is left empty (null) when it would be meaningless, e.g. dividing by
zero or by negative equity.

Also adds: sector (from the SIC code), flags for banks/insurers and for
shell companies (SPACs), and whether the company is based in Ireland.

Writes: company_ratios  (PostgreSQL + blob processed/company_ratios/)
"""
from pyspark.sql import functions as F

from src.spark.session import blob, get_spark, write_table


def safe_div(num, den, positive_den=True):
    """num / den, or null when the denominator is zero, missing or (optionally) negative."""
    cond = (F.col(den) > 0) if positive_den else (F.col(den) != 0)
    return F.when(cond, F.col(num) / F.col(den))


def sector(sic):
    """SEC industry divisions, from the 4-digit SIC code."""
    return (F.when(sic.between(100, 999), "Agriculture")
             .when(sic.between(1000, 1499), "Mining & Energy")
             .when(sic.between(1500, 1799), "Construction")
             .when(sic.between(2000, 3999), "Manufacturing")
             .when(sic.between(4000, 4999), "Transport & Utilities")
             .when(sic.between(5000, 5199), "Wholesale")
             .when(sic.between(5200, 5999), "Retail")
             .when(sic.between(6000, 6799), "Finance & Real Estate")
             .when(sic.between(7000, 8999), "Services")
             .otherwise("Other"))


def main():
    spark = get_spark("compute-ratios")
    df = spark.read.parquet(blob("processed", "company_years"))

    ratios = (df
        .withColumn("net_margin", safe_div("net_income", "revenue"))
        .withColumn("roa", safe_div("net_income", "total_assets"))
        .withColumn("roe", safe_div("net_income", "equity"))
        .withColumn("interest_coverage", safe_div("operating_income", "interest_expense"))
        .withColumn("cost_of_debt", safe_div("interest_expense", "long_term_debt"))
        .withColumn("debt_to_equity", safe_div("long_term_debt", "equity"))
        .withColumn("debt_to_assets", safe_div("total_liabilities", "total_assets"))
        .withColumn("current_ratio", safe_div("current_assets", "current_liabilities"))
        .withColumn("cash_to_assets", safe_div("cash", "total_assets"))
        # Altman Z'' (version for non-manufacturers and private firms):
        #   6.56 x working capital/assets + 3.26 x retained earnings/assets
        # + 6.72 x operating income/assets + 1.05 x equity/liabilities
        .withColumn("altman_z",
            6.56 * (F.col("current_assets") - F.col("current_liabilities")) / F.col("total_assets")
            + 3.26 * F.col("retained_earnings") / F.col("total_assets")
            + 6.72 * F.col("operating_income") / F.col("total_assets")
            + 1.05 * F.col("equity") / F.col("total_liabilities"))
        .withColumn("altman_z", F.when(F.col("total_assets") > 0, F.col("altman_z")))
        .withColumn("health_zone",
            F.when(F.col("altman_z") > 2.6, "safe")
             .when(F.col("altman_z") >= 1.1, "grey")
             .when(F.col("altman_z").isNotNull(), "distress"))
        .withColumn("sector", sector(F.col("sic")))
        # Banks and insurers: debt is their raw material, so debt ratios mean
        # something different -> excluded from debt analyses.
        .withColumn("is_financial", F.col("sic").between(6000, 6799))
        # SIC 6770 = blank-check companies (SPACs): shells with no real business.
        .withColumn("is_shell", F.col("sic") == 6770)
        .withColumn("is_ireland", F.col("country") == "IE"))

    ratios.cache()
    print(f"Rows: {ratios.count():,}")
    print("How many company-years have each key ratio:")
    ratios.select([F.count(c).alias(c) for c in
                   ["net_margin", "roe", "interest_coverage", "current_ratio", "altman_z"]]).show()
    print("Median ratios by year (non-financial companies):")
    (ratios.filter("NOT is_financial AND NOT is_shell")
           .groupBy("fy", "avg_policy_rate")
           .agg(F.expr("percentile_approx(net_margin, 0.5)").alias("median_net_margin"),
                F.expr("percentile_approx(interest_coverage, 0.5)").alias("median_interest_cover"),
                F.expr("percentile_approx(cost_of_debt, 0.5)").alias("median_cost_of_debt"))
           .orderBy("fy").show())
    print(f"Ireland-based company-years: {ratios.filter('is_ireland').count():,}")
    write_table(ratios, "company_ratios")
    spark.stop()


if __name__ == "__main__":
    main()
