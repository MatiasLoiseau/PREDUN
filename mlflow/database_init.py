import os
import mlflow
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

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
    ("data-private/porcentaje_avance_100_por_100.csv", "AVANCE_2024_1C"),
    ("data-private/listado_alumnos_final.csv",         "ALUMNOS_2024_1C"),
    ("data-private/CENSALES.csv",                      "CENSALES_2024_1C"),
    ("data-private/CURSADA_HISTORICA_final.csv",       "CURSADA_HISTORICA_2024_1C")
]

# MLflow experiment name
EXPERIMENT_NAME = "CSV_Upload_to_PostgreSQL"

# Run tag
DATA_VERSION_TAG = "2024_1C_v1"

def create_db_engine(db_params):
    """
    Builds the PostgreSQL connection URL and returns a SQLAlchemy engine.
    """
    connection_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    return create_engine(connection_url)

def upload_csv_to_postgres(csv_path, table_name, engine, schema):
    """
    Reads a CSV using pandas and uploads it to the PostgreSQL table 'schema.table_name'.
    If the table exists, it replaces it ('if_exists'='replace').
    You can change it to 'append' or 'fail' based on your needs.
    """
    df = pd.read_csv(csv_path)
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists='replace',  # options: 'fail', 'replace', 'append'
        index=False
    )

def main():
    mlflow.set_tracking_uri("http://localhost:8002")
    mlflow.set_experiment(EXPERIMENT_NAME)
    engine = create_db_engine(DB_PARAMS)

    with mlflow.start_run(run_name=f"Data Upload - {DATA_VERSION_TAG}"):
        mlflow.log_param("data_version", DATA_VERSION_TAG)
        for csv_file, new_table_name in FILES_TO_UPLOAD:
            upload_csv_to_postgres(
                csv_path=csv_file,
                table_name=new_table_name,
                engine=engine,
                schema=DB_PARAMS["schema"]
            )
            mlflow.log_artifact(csv_file, artifact_path=f"data_files/{DATA_VERSION_TAG}")
            mlflow.log_param(f"table_{new_table_name}", csv_file)

            print(f"File '{csv_file}' uploaded to table '{new_table_name}'.")

        total_rows = 0
        for csv_file, _ in FILES_TO_UPLOAD:
            df_temp = pd.read_csv(csv_file)
            total_rows += len(df_temp)

        mlflow.log_metric("total_rows_uploaded", total_rows)

        print(f"\nA total of {total_rows} rows were uploaded from all CSV files.")

    print("Data upload process completed and logged in MLflow.")

if __name__ == "__main__":
    main()

