#!/bin/bash
# =====================================================================
#  Who was hurt by rising interest rates? - full pipeline, one command
#
#  Runs every step in order and stops at the first failure:
#    0. start the cluster and wait until every node is ready
#    1. collect the data           (SEC files + 2 APIs -> Azure blob "raw")
#    2. MapReduce                  (Hadoop: filter to annual line items)
#    3. Spark: company-year table and financial ratios
#    4. Spark: research questions   (results -> PostgreSQL + blob "processed")
#    5. follow-up analysis         (only once it exists, Phase 6)
#
#  Usage, from the project folder:
#    ./run_pipeline.sh            full run
#    ./run_pipeline.sh --test     quick run on 2 quarters only (for checks)
#
#  A log of every run is kept in logs/.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p logs
LOG="logs/run_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1      # everything below is also written to the log

QUARTERS=""
if [[ "${1:-}" == "--test" ]]; then QUARTERS="2023q1 2024q1"; fi

START=$(date +%s)
step() { echo; echo "######## $1   ($(date +%H:%M:%S), $(( $(date +%s) - START ))s since start)"; }

# Python steps run on this machine, in the project's own Python environment.
if [[ -f .venv/bin/activate ]]; then source .venv/bin/activate; fi
[[ -f .env ]] || { echo "Missing .env file: copy .env.example to .env and fill it in."; exit 1; }

step "0. Starting the cluster"
docker compose up -d
echo -n "Waiting for 2 HDFS DataNodes"
# (The report is saved first and then searched: "cmd | grep -q" can report a
#  false failure when the script runs with "set -o pipefail".)
until report=$(docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null) \
      && [[ "$report" == *"Live datanodes (2)"* ]]; do
  echo -n "."; sleep 5
done; echo " ready"
echo -n "Waiting for 2 Spark workers"
until [[ $(curl -s http://localhost:8080/json/ 2>/dev/null | grep -o '"state" *: *"ALIVE"' | wc -l || true) -ge 2 ]]; do
  echo -n "."; sleep 5
done; echo " ready"

step "1. Collecting data (SEC statements, SEC company API, NY Fed rates API)"
python3 -m src.ingest.run_all $QUARTERS

step "2a. Preparing MapReduce inputs"
python3 -m src.mapreduce.prepare_inputs

step "2b. Running MapReduce on Hadoop"
bash src/mapreduce/run_mapreduce.sh

step "3-4. Running Spark jobs"
bash src/spark/submit_all.sh

if [[ -f analysis/followup_analysis.py ]]; then
  step "5. Follow-up analysis"
  python3 -m analysis.followup_analysis
fi

step "Pipeline finished in $(( ($(date +%s) - START) / 60 )) minutes. Log: $LOG"
