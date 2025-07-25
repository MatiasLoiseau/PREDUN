#!/bin/bash

# Usage: ./run_ingestion.sh <pg_user> <pg_password> <version: 2024_2C | 2025_1C>

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <pg_user> <pg_password> <version: 2024_2C | 2025_1C>"
  exit 1
fi

PG_USER=$1
PG_PASS=$2
VERSION=$3
PG_CONN="postgresql://${PG_USER}:${PG_PASS}@localhost:5432/postgres"

if [[ "$VERSION" != "2024_2C" && "$VERSION" != "2025_1C" ]]; then
  echo "Invalid version: $VERSION"
  echo "Allowed versions are: 2024_2C or 2025_1C"
  exit 1
fi

echo "Running ingestion for version $VERSION..."

# Preprocessing scripts
python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/fix_and_clean/v${VERSION}.yaml
python ingestion/02_format_students_pre_ingestion.py ingestion/mappings/fix_and_clean/students_v${VERSION}.yml
python ingestion/03_format_percentage_pre_ingestion.py ingestion/mappings/fix_and_clean/percentage_v${VERSION}.yml

# Ingest to staging
python ingestion/04_ingest_to_staging.py --period $VERSION --root data-private --pg "$PG_CONN"

# DBT: compile, install dependencies and run models
cd predun_dbt || { echo "Failed to change directory to predun_dbt"; exit 1; }

echo "Running ./recompile_dbt.sh..."
./recompile_dbt.sh

echo "Running dbt run for canonical.*..."
dbt run --select "canonical.*"
cd ..

echo "Ingestion for $VERSION completed successfully."