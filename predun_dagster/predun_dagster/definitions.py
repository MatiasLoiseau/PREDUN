import os
from dagster import Definitions

from .assets import (
    dbt_project_assets,
    train_student_dropout_model,
    score_student_dropout_risk,
    format_history_data,
    format_students,
    format_percentage,
    ingest_to_staging,
)
from .resources import PostgresResource, MLflowResource
from .jobs import refresh_canonical
from .jobs_ingestion import ingestion_job
from .sensors import new_period_sensor
from .constants import PG_URI_ENV, DBT_PROJECT_DIR, DBT_PROFILES_DIR
from dagster_dbt import DbtCliResource

# Resources ------------------------------------------------------------------
pg_resource = PostgresResource(
    conn_uri=os.getenv(
        PG_URI_ENV,
        f"postgresql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST','localhost')}:5432/"
        f"{os.getenv('DB_NAME','postgres')}",
    )
)

mlflow_resource = MLflowResource()

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# Definitions ----------------------------------------------------------------
defs = Definitions(
    assets=[
        dbt_project_assets,
        train_student_dropout_model,
        score_student_dropout_risk,
        format_history_data,
        format_students,
        format_percentage,
        ingest_to_staging,
    ],
    resources={
        "dbt": dbt_resource,
        "postgres": pg_resource,
        "mlflow": mlflow_resource,
    },
    jobs=[refresh_canonical, ingestion_job],
    sensors=[new_period_sensor],
)