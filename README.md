# PREDUN: Predicting Student Dropout Rates at UNDAV Using MLOps

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Stable Release](https://img.shields.io/badge/development-v0.2.0-brightgreen.svg)](https://github.com/your-repo/releases)

PREDUN stands for "Predicción de Deserción Universitaria en UNDAV".

---

## Table of Contents

- [Project Overview](#project-overview)
- [Dataset Information](#dataset-information)
- [Folder Structure](#folder-structure)
- [Installation](#installation)
- [Environment Variables](#environment-variables)
- [PostgreSQL Setup](#postgresql-setup)
- [MLflow Usage](#mlflow-usage)
- [Data Ingestion](#data-ingestion)
- [DBT Usage](#dbt-usage)
- [Dagster Usage](#dagster-usage)
- [Meltano Usage](#meltano-usage)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Project Overview

This project develops and deploys machine learning models to predict student dropout rates at the Universidad Nacional de Avellaneda (UNDAV). By applying MLOps principles, PREDUN aims to provide a scalable, automated, and reproducible system for identifying students at risk, enabling timely interventions by university staff.

## Dataset Information

The dataset contains academic records for UNDAV students. Each row represents a student's course enrollment or exam result. Key attributes include:

- Student ID
- Course code
- Enrollment date
- Exam results
- Progress percentage
- Categorical features (e.g., gender, program)

Mappings are used to standardize categorical data for model training. See the `ingestion/mappings/fix_and_clean/` folder for details.

## Folder Structure

```
ingestion/            # Data cleaning and ingestion scripts
models/               # ML models
predun_dagster/       # Dagster pipelines
predun_dbt/           # DBT models and macros
predun_meltano/       # Meltano data pipeline for INDEC Argentina APIs
utils/                # Utility scripts
```

## Installation

```bash
conda create -n dagster-predun python=3.9 -y
conda activate dagster-predun
pip install -r requirements-dagster.txt
pip install -e .
```

## Environment Variables

Create a `.env` file in your project directory:

```ini
REPO_FOLDER=${PWD}
POSTGRES_DATA_FOLDER=/path/to/your/postgres/data
POSTGRES_USER=example_user
POSTGRES_PASSWORD=example_password
POSTGRES_HOST=localhost:5432
MLFLOW_POSTGRES_DB=mlflow_db
MLFLOW_ARTIFACTS_PATH=/path/to/your/mlflow/artifacts
AIRBYTE_USER=example_airbyte_user
AIRBYTE_PASSWORD=example_airbyte_password
DB_HOST=localhost
DB_NAME=example_db
DB_USER=example_user
DB_PASSWORD=example_password
DB_SCHEMA=example_schema
PG_URI="postgresql://user:password@localhost:5432/user"
```

Load the environment variables:

```bash
set -o allexport && source .env && set +o allexport
```

## PostgreSQL Setup

### 1. Pull and Run PostgreSQL Docker Container

```bash
docker pull postgres
docker run -d \
    --name predun-postgres \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v $POSTGRES_DATA_FOLDER:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres
```

### 2. Manage the Container

```bash
docker ps
docker exec -it predun-postgres /bin/bash
psql -U postgres
```

### 3. (Optional) Install PostgreSQL Client

```bash
sudo apt install postgresql-client-16
export PGPASSWORD=$POSTGRES_PASSWORD
psql -U postgres -h localhost -p 5432
```

### 4. Create MLflow Database and Users

```sql
CREATE DATABASE mlflow_db;
CREATE USER mlflow_user WITH ENCRYPTED PASSWORD 'mlflow';
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO mlflow_user;
CREATE SCHEMA example_schema;
CREATE USER "example_user" WITH ENCRYPTED PASSWORD 'example_password';
GRANT ALL PRIVILEGES ON DATABASE postgres TO "example_user";
GRANT ALL ON SCHEMA example_schema TO "example_user";
```

## MLflow Usage

Start the PostgreSQL container:

```bash
docker start -a predun-postgres
```

Load environment variables:

```bash
set -o allexport && source .env && set +o allexport
```

Run MLflow server:

```bash
mlflow server \
    --backend-store-uri postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$MLFLOW_POSTGRES_DB \
    --default-artifact-root $MLFLOW_ARTIFACTS_PATH \
    -h 0.0.0.0 \
    -p 8002
```

## Data Ingestion

### 1. Preprocess and Clean Data

```bash
python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/fix_and_clean/v2024_2C.yaml
python ingestion/02_format_students_pre_ingestion.py ingestion/mappings/fix_and_clean/students_v2024_2C.yml
python ingestion/03_format_percentage_pre_ingestion.py ingestion/mappings/fix_and_clean/percentage_v2024_2C.yml
```

### 2. Ingest to Staging

```bash
python ingestion/04_ingest_to_staging.py --period 2024_2C --root data-private --pg "postgresql://user:password@localhost:5432/postgres"
```

### (Optional) Run All Ingestion Steps

```bash
./utils/run_ingestion.sh user_db password_db version # e.g. 2024_2C
```

## DBT Usage

```bash
cd predun_dbt
# Optional: ./recompile_dbt.sh
dbt run --select "canonical.*"
dbt run --select marts.student_status
dbt run --select marts.student_panel
```

## Dagster Usage

1. **Start the Dagster web interface:**

```bash
cd predun_dagster
dagster dev -m predun_dagster
```

Access the Dagster UI at: [http://localhost:3000](http://localhost:3000)

2. **Run the Ingestion Job**

In the Dagster UI, launch the ingestion job with the following configuration (replace `VERSION`, `user`, and `password` as needed):

```yaml
ops:
  format_history_data:
    config:
      version: "VERSION"
  format_students:
    config:
      version: "VERSION"
  format_percentage:
    config:
      version: "VERSION"
  ingest_to_staging:
    config:
      version: "VERSION"
      pg_user: "user"
      pg_password: "password"
```

Set `version` to the desired period (e.g., `2024_2C` or `2025_1C`) and provide your PostgreSQL username and password.

3. **Run the Refresh Canonical Job**

After ingestion, execute the `refresh_canonical` job from the Dagster UI to update canonical tables.

## Meltano Usage

The `predun_meltano` module implements a data pipeline to extract economic indicators from INDEC Argentina APIs and load them into PostgreSQL.

### Datasets Extracted

1. **IPC by Categories** (Consumer Price Index)
   - Nuclear, Regulated, and Seasonal categories
   - Monthly frequency, base year 2016

2. **EPH Unemployment Rate** (Gran San Juan)
   - Quarterly unemployment rates
   - Continuous Household Survey data

3. **EMAE Indicators** (Monthly Economic Activity Estimator)
   - General level economic activity
   - Monthly frequency

### Setup and Usage

1. **Navigate to Meltano directory:**

```bash
cd predun_meltano
```

2. **Install dependencies:**

```bash
pip install -r requirements.txt
```

3. **Create PostgreSQL tables:**

Execute the SQL commands in `create_tables.sql` to create the necessary tables in the `canonical` schema:

```bash
psql -U postgres -h localhost -p 5432 -f create_tables.sql
```

4. **Run the complete pipeline:**

```bash
python indec_pipeline.py
```

This will:
- Extract data from INDEC APIs
- Load data into PostgreSQL canonical schema
- Create tables: `canonical.ipc_categories`, `canonical.unemployment_rates`, `canonical.emae_indicators`

### Manual Steps

You can also run the pipeline manually:

```bash
# Extract and load data directly from INDEC APIs to PostgreSQL
python indec_pipeline.py
```

For detailed information, see the [Meltano README](predun_meltano/README.md).

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
