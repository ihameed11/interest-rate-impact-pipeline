#!/bin/bash
# Runs the Spark jobs in order on the Spark cluster; stops if one fails.
# Run from the project folder with the cluster up:
#   bash src/spark/submit_all.sh            # all jobs
#   bash src/spark/submit_all.sh compute_ratios   # one job
set -euo pipefail
cd "$(dirname "$0")/../.."

JOBS=("${@:-build_company_years compute_ratios}")
for job in ${JOBS[@]}; do
  echo "=================== Spark job: $job ==================="
  docker compose exec -T spark-master spark-submit /opt/project/src/spark/$job.py
done
echo "All Spark jobs finished."
