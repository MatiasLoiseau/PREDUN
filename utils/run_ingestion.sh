#!/bin/bash

# Usage: ./run_ingestion.sh <pg_user> <pg_password> <step: 1 | 2 | 3 | all>

# Check that 3 arguments are provided
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <pg_user> <pg_password> <step: 1 | 2 | 3 | all>"
  exit 1
fi

PG_USER=$1
PG_PASS=$2
STEP=$3
PG_CONN="postgresql://${PG_USER}:${PG_PASS}@localhost:5432/postgres"

run_step_1() {
  echo "Step 1: Data preprocessing"
  python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/fix_and_clean/v2024_2C.yaml
  python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/fix_and_clean/v2025_1C.yaml

  python ingestion/02_format_students_pre_ingestion.py ingestion/mappings/fix_and_clean/students_v2024_2C.yml
  python ingestion/02_format_students_pre_ingestion.py ingestion/mappings/fix_and_clean/students_v2025_1C.yml

  python ingestion/03_format_percentage_pre_ingestion.py ingestion/mappings/fix_and_clean/percentage_v2024_2C.yml
  python ingestion/03_format_percentage_pre_ingestion.py ingestion/mappings/fix_and_clean/percentage_v2025_1C.yml
}

run_step_2() {
  echo "Step 2: Load data into staging"
  python ingestion/04_ingest_to_staging.py --period 2024_2C --root data-private --pg "$PG_CONN"
  python ingestion/04_ingest_to_staging.py --period 2025_1C --root data-private --pg "$PG_CONN"
}

run_step_3() {
  echo "Step 3: Refresh canonical tables"
  python predun_dbt/scripts/refresh_canonical.py --project-dir predun_dbt/
}

# Execute steps based on the input argument
case "$STEP" in
  1)
    run_step_1
    ;;
  2)
    run_step_2
    ;;
  3)
    run_step_3
    ;;
  all)
    run_step_1
    run_step_2
    run_step_3
    ;;
  *)
    echo "Invalid step: $STEP"
    echo "Allowed values are: 1, 2, 3, all"
    exit 1
    ;;
esac

echo "Script completed successfully."