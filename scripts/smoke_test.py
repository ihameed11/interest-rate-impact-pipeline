"""
Smoke test for the local cluster.

Proves that the three main parts can talk to each other:
  1. Spark reads a file from HDFS (Hadoop storage)
  2. Spark does a tiny calculation on its workers
  3. Spark writes the result straight into PostgreSQL

Run from the project folder:
  docker compose exec spark-master spark-submit /opt/project/scripts/smoke_test.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("smoke-test").getOrCreate()

# 1. Read the test file we put into HDFS earlier (/test.txt).
lines = spark.read.text("hdfs://namenode:9000/test.txt")

# 2. Count the words in it (runs on the Spark workers).
words = (
    lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
    .groupBy("word")
    .count()
)
words.show()

# 3. Write the result into PostgreSQL, in a table called smoke_test.
jdbc_url = "jdbc:postgresql://postgres:5432/" + os.environ["POSTGRES_DB"]
words.write.jdbc(
    url=jdbc_url,
    table="smoke_test",
    mode="overwrite",
    properties={
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
    },
)
print("SMOKE TEST PASSED: HDFS -> Spark -> PostgreSQL")
spark.stop()
