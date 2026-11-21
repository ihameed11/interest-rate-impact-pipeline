#!/bin/bash
# Runs the Spark jobs in order on the Spark cluster; stops if one fails.
# Run from the project folder with the cluster up:
#   bash src/spark/submit_all.sh            # all jobs
#   bash src/spark/submit_all.sh rq1_interest_burden   # one job
set -euo pipefail
cd "$(dirname "$0")/../.."

JOBS=("${@:-build_company_years compute_ratios rq1_interest_burden rq2_profitability rq3_financial_health}")
for job in ${JOBS[@]}; do
  echo "=================== Spark job: $job ==================="
  docker compose exec -T spark-master spark-submit /opt/project/src/spark/$job.py
done
echo "All Spark jobs finished."
