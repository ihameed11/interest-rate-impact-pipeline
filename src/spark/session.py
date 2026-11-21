"""
Shared start-up code for every Spark job.

Gives each job:
  - a Spark session that can read/write Azure Blob Storage directly
  - blob(...)      -> the abfss:// address of a file or folder in blob storage
  - write_table()  -> saves a result to PostgreSQL AND as Parquet in blob storage
"""
import os

from pyspark.sql import DataFrame, SparkSession

ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]


def get_spark(app_name: str) -> SparkSession:
    key = os.environ["AZURE_STORAGE_KEY"]
    spark = (
        SparkSession.builder.appName(app_name)
        # Log in to the storage account with its access key.
        .config(f"spark.hadoop.fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", key)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def blob(container: str, path: str) -> str:
    """Address Spark uses for a file or folder in Azure Blob Storage."""
    return f"abfss://{container}@{ACCOUNT}.dfs.core.windows.net/{path}"


def write_table(df: DataFrame, name: str) -> None:
    """
    Save a result in two places:
      1. PostgreSQL table <name>       -> for SQL queries and the follow-up analysis
      2. blob processed/<name>/ (Parquet) -> the "after processing" copy in blob storage
    """
    jdbc_url = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"
    (df.write.mode("overwrite")
       .option("truncate", "true")  # keep the table, replace its rows
       .jdbc(jdbc_url, name, properties={
           "user": os.environ["POSTGRES_USER"],
           "password": os.environ["POSTGRES_PASSWORD"],
           "driver": "org.postgresql.Driver",
       }))
    df.write.mode("overwrite").parquet(blob("processed", name))
    print(f"  saved {name}: PostgreSQL table + blob processed/{name}/")
