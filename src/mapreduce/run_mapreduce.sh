#!/bin/bash
# Step 2b: run the MapReduce job on the Hadoop cluster.
#
#   1. copy the prepared inputs into HDFS (the cluster's file system)
#   2. run the Python mapper and reducer on YARN with Hadoop Streaming
#   3. bring the result back and store it in blob storage ("processed")
#
# Run from the project folder, with the cluster up and .venv active:
#   bash src/mapreduce/run_mapreduce.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

NN="docker compose exec -T namenode bash -c"
P=/opt/project   # the project folder as seen inside the containers

echo "== 1. Copying inputs into HDFS"
$NN "hdfs dfs -mkdir -p /input/num && hdfs dfs -put -f $P/data/staging/num/*.txt.gz /input/num/"
$NN "hdfs dfs -ls -h /input/num | tail -5"

echo "== 2. Running the MapReduce job (watch it at http://localhost:8088)"
$NN "hdfs dfs -rm -r -f /output/facts"
$NN "hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name=sec-annual-line-items \
  -D mapreduce.job.reduces=2 \
  -files $P/src/mapreduce/facts_mapper.py,$P/src/mapreduce/facts_reducer.py,$P/data/staging/filings_10k.tsv,$P/data/staging/num_headers.tsv,$P/src/common/line_items.csv \
  -mapper 'python3 facts_mapper.py' \
  -reducer 'python3 facts_reducer.py' \
  -input /input/num \
  -output /output/facts"

echo "== 3. Collecting the result"
$NN "hdfs dfs -getmerge /output/facts $P/data/staging/facts.tsv"
echo "Rows in result: $(wc -l < data/staging/facts.tsv)"
python3 -m src.mapreduce.upload_output
