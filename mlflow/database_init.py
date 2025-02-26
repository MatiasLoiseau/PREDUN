import os
import time
import mlflow
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import json

load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": "5432",
    "database": os.getenv("DB_NAME"),
    "schema": os.getenv("DB_SCHEMA"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

FILES_TO_UPLOAD = [
    ("../data-private/porcentaje_avance_100_por_100.csv", "AVANCE_2024_1C"),
    ("../data-private/listado_alumnos_final.csv",         "ALUMNOS_2024_1C"),
    ("../data-private/CENSALES.csv",                      "CENSALES_2024_1C"),
    ("../data-private/CURSADA_HISTORICA_final.csv",       "CURSADA_HISTORICA_2024_1C")
]

EXPERIMENT_NAME = "CSV_Upload_to_PostgreSQL"
DATA_VERSION_TAG = "2024_1C_v3"

def create_db_engine(db_params):
    connection_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    return create_engine(connection_url)

def upload_csv_to_postgres(csv_path, table_name, engine, schema):
    df = pd.read_csv(csv_path)
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='replace',  # options: 'fail', 'replace', 'append'
        index=False
    )
    return df

def main():
    mlflow.set_tracking_uri("http://localhost:8002")
    mlflow.set_experiment(EXPERIMENT_NAME)
    engine = create_db_engine(DB_PARAMS)

    with mlflow.start_run(run_name=f"Data Upload - {DATA_VERSION_TAG}"):
        mlflow.log_param("data_version", DATA_VERSION_TAG)
        mlflow.log_param("db_host", DB_PARAMS["host"])
        mlflow.log_param("db_name", DB_PARAMS["database"])
        mlflow.log_param("db_schema", DB_PARAMS["schema"])

        total_rows = 0

        for csv_file, new_table_name in FILES_TO_UPLOAD:
            file_size = os.path.getsize(csv_file) / (1024 * 1024)  # Size in MB
            start_time = time.time()
            df = upload_csv_to_postgres(csv_file, new_table_name, engine, DB_PARAMS["schema"])
            elapsed_time = time.time() - start_time
            row_count = len(df)

            mlflow.log_param(f"table_{new_table_name}", csv_file)
            mlflow.log_metric(f"rows_{new_table_name}", row_count)
            mlflow.log_metric(f"time_{new_table_name}_sec", round(elapsed_time, 2))
            mlflow.log_param(f"columns_{new_table_name}", ", ".join(df.columns))
            mlflow.log_metric(f"file_size_{new_table_name}_MB", round(file_size, 2))
            mlflow.log_metric(f"missing_values_{new_table_name}", df.isnull().sum().sum())
            mlflow.log_metric(f"duplicate_rows_{new_table_name}", df.duplicated().sum())

            sample_df = df.sample(n=min(10, len(df)))
            sample_json = sample_df.to_json(orient="records")
            mlflow.log_text(sample_json, artifact_file=f"data_samples/{DATA_VERSION_TAG}/{new_table_name}_sample.json")

            schema_json = json.dumps(df.dtypes.apply(str).to_dict())
            mlflow.log_text(schema_json, artifact_file=f"data_schemas/{DATA_VERSION_TAG}/{new_table_name}_schema.json")

            total_rows += row_count
            print(f"File '{csv_file}' uploaded to table '{new_table_name}' with {row_count} rows.")

        mlflow.log_metric("total_rows_uploaded", total_rows)
        print(f"\nA total of {total_rows} rows were uploaded from all CSV files.")

    print("Data upload process completed and logged in MLflow.")

if __name__ == "__main__":
    main()
