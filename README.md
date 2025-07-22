# Predicting Student Dropout Rates at UNDAV Using MLOps

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)[![Stable Release](https://img.shields.io/badge/development-v0.0.1-brightgreen.svg)](https://github.com/your-repo/releases)
<!--[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/release/python-380/)-->

PREDUN is an acronym for "Predicción de Deserción Universitaria en UNDAV".

## Project Overview

This project aims to develop and deploy machine learning models to predict student dropout rates at the Universidad Nacional de Avellaneda (UNDAV). By utilizing MLOps principles, the goal is to create a scalable, automated, and reproducible system that can predict which students are at risk of dropping out, thereby enabling timely interventions by the university administration.

## Dataset Information

The dataset used in this project contains academic records for students enrolled at UNDAV. Each row represents a unique record associated with a student's course enrollment or exam result. Key attributes are as follows:

These mappings ensure consistency and facilitate model training by transforming categorical data into standardized numerical values.

## Installation

### 1. Create the Conda Environment

```bash
conda create -n mlflow-predun python=3.12
conda activate mlflow-predun
pip install mlflow
conda install -c conda-forge psycopg2 python-dotenv
```

### 2. Setting Up Environment Variables

To configure the necessary environment variables, create a `.env` file in your project directory with the following content:

```
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

To load the environment variables, execute the following command:

```bash
set -o allexport && source .env && set +o allexport
```

### 3. Setting Up PostgreSQL

#### 1. Pull the Docker Image

```bash
docker pull postgres
```

#### 2. Run the PostgreSQL Container

```bash
docker run -d \
    --name predun-postgres \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v $POSTGRES_DATA_FOLDER:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres
```

#### 3. Verify and Manage the Container

Common Docker commands:

```bash
docker ps
docker ps -a
docker exec -it predun-postgres /bin/bash
```

Access PostgreSQL inside the container:

```bash
psql -U postgres
```

#### (Optional) Install PostgreSQL Client

```bash
sudo apt install postgresql-client-16
export PGPASSWORD=$POSTGRES_PASSWORD
psql -U postgres -h localhost -p 5432
```

#### 4. Create the MLFlow Database

Run these SQL commands to set up the database:

```sql
CREATE DATABASE mlflow_db;
CREATE USER mlflow_user WITH ENCRYPTED PASSWORD 'mlflow';
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO mlflow_user;
CREATE SCHEMA example_schema;
CREATE USER "example_user" WITH ENCRYPTED PASSWORD 'example_password';
GRANT ALL PRIVILEGES ON DATABASE postgres TO "example_user";
GRANT ALL ON SCHEMA example_schema TO "example_user";
```

## Run Docker

```bash
docker start -a predun-postgres
```

## Run MLflow

Remember load the environment variables

```bash
set -o allexport && source .env && set +o allexport
```
```bash
mlflow server \
    --backend-store-uri postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST/$MLFLOW_POSTGRES_DB \
    --default-artifact-root $MLFLOW_ARTIFACTS_PATH \
    -h 0.0.0.0 \
    -p 8002 
```

## Run Ingestion

### Part 1: Fix, clean and format

To preprocess and clean the input data using metadata-driven mappings, execute the following scripts:

```bash
python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/fix_and_clean/v2024_2C.yaml
python ingestion/02_format_students_pre_ingestion.py ingestion/mappings/fix_and_clean/students_v2024_2C.yml
python ingestion/03_format_percentage_pre_ingestion.py ingestion/mappings/fix_and_clean/percentage_v2024_2C.yml
```

### Part 2: Ingest to staging

Add every data from csv to staging tables

```bash
python ingestion/04_ingest_to_staging.py --period 2024_2C --root data-private --pg "postgresql://user:password@localhost:5432/postgres"
```

## Run DBT

```bash
cd predun_dbt
# optional: ./recompile_dbt
dbt run --select "canonical.*" 
```

### Run marts

```bash
dbt run --select marts.student_status
dbt run --select marts.student_panel
```

## Optional: Run ingestion in one step

```bash
./utils/run_ingestion.sh user_db password_db version #2024_2C
```

## Things to do

- Automate the installation (try uv instead of pip or conda?)

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more details.
