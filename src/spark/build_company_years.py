"""
Spark job 3a: build ONE ROW PER COMPANY PER FISCAL YEAR.

Reads from Azure Blob Storage:
  processed/mapreduce/facts.tsv          MapReduce result: cik, year, tag, value
  processed/reference/filings_10k.tsv    annual reports: company name, sector code, country
  raw/sec_companies/*.json               SEC company list (API): ticker, stock exchange
  raw/rates/effr_daily.json              NY Fed daily interest rate (API)

Steps:
  1. For each financial line item pick the best available SEC tag
     (e.g. revenue is reported under 4 different tags; see line_items.csv).
  2. Turn the long list (one row per number) into a wide table
     (one row per company-year, one column per line item).
  3. Add company details and the average interest rate of that year.

Writes: company_years  (PostgreSQL + blob processed/company_years/)
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.spark.session import blob, get_spark, write_table

FIRST_YEAR, LAST_YEAR = 2019, 2025   # 2019 = pre-COVID baseline


def main():
    spark = get_spark("build-company-years")

    # ---- 1. MapReduce output (tab-separated, no header)
    facts = (spark.read.csv(blob("processed", "mapreduce/facts.tsv"), sep="\t")
             .toDF("cik", "fy", "tag", "value", "adsh", "filed")
             .withColumn("fy", F.col("fy").cast("int"))
             .withColumn("value", F.col("value").cast("double"))
             .filter(F.col("fy").between(FIRST_YEAR, LAST_YEAR)))

    # ---- 2. Which tag to use for each line item (small list, read on the driver)
    with open("/opt/project/src/common/line_items.csv") as f:
        rows = [(r["line_item"], r["tag"], int(r["priority"])) for r in csv.DictReader(f)]
    line_items = spark.createDataFrame(rows, ["line_item", "tag", "priority"])

    # Keep the highest-priority tag each company actually reported.
    best = Window.partitionBy("cik", "fy", "line_item").orderBy("priority")
    picked = (facts.join(F.broadcast(line_items), "tag")
              .withColumn("rank", F.row_number().over(best))
              .filter("rank = 1"))

    # Long -> wide: one column per line item.
    wide = picked.groupBy("cik", "fy").pivot("line_item").agg(F.first("value"))

    # Long-term debt: use the total if reported, otherwise current + non-current parts.
    wide = wide.withColumn(
        "long_term_debt",
        F.coalesce(F.col("long_term_debt"),
                   F.coalesce(F.col("long_term_debt_noncurrent"), F.lit(0.0))
                   + F.coalesce(F.col("long_term_debt_current"), F.lit(0.0))))

    # ---- 3a. Company details from the annual reports (latest filing per year)
    filings = spark.read.csv(blob("processed", "reference/filings_10k.tsv"),
                             sep="\t", header=True)
    latest = Window.partitionBy("cik", "fy").orderBy(F.col("filed").desc())
    companies = (filings.withColumn("fy", F.col("fy").cast("int"))
                 .withColumn("rank", F.row_number().over(latest)).filter("rank = 1")
                 .select("cik", "fy", "name", F.col("sic").cast("int").alias("sic"),
                         F.col("countryba").alias("country"), F.col("stprba").alias("state")))

    # ---- 3b. Ticker and exchange from the SEC company list (API answer, nested JSON)
    sec_json = spark.read.json(blob("raw", "sec_companies/*.json"), multiLine=True)
    tickers = (sec_json.select(F.explode("data").alias("d"))
               .select(F.col("d")[0].cast("long").cast("string").alias("cik"),
                       F.col("d")[2].alias("ticker"), F.col("d")[3].alias("exchange"))
               .dropDuplicates(["cik"]))

    # ---- 3c. Average interest rate per calendar year (API answer, nested JSON)
    rates_json = spark.read.json(blob("raw", "rates/effr_daily.json"), multiLine=True)
    rates = (rates_json.select(F.explode("refRates").alias("r"))
             .select(F.year(F.to_date("r.effectiveDate")).alias("fy"),
                     F.col("r.percentRate").cast("double").alias("rate"))
             .groupBy("fy").agg(F.round(F.avg("rate"), 2).alias("avg_policy_rate")))

    # ---- Put it all together
    result = (wide.join(companies, ["cik", "fy"], "left")
              .join(tickers, "cik", "left")
              .join(rates, "fy", "left")
              .withColumn("rate_period",
                          F.when(F.col("fy") <= 2021, "low")
                           .when(F.col("fy") == 2022, "rising")
                           .otherwise("high"))
              .drop("long_term_debt_noncurrent", "long_term_debt_current"))

    result.cache()
    print(f"Company-years: {result.count():,}  |  companies: {result.select('cik').distinct().count():,}")
    result.groupBy("fy", "avg_policy_rate").count().orderBy("fy").show()
    write_table(result, "company_years")
    spark.stop()


if __name__ == "__main__":
    main()
