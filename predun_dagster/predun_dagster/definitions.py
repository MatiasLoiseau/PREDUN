import os
from dagster import Definitions
import socket
import datetime

from .assets import (
    dbt_project_assets,
    train_student_dropout_model,
    predict_student_dropout_risk,
    format_history_data,
    format_students,
    format_percentage,
    ingest_to_staging,
)
from .resources import PostgresResource, MLflowResource
from .monitoring import MLFlowMonitoringResource
from .jobs import refresh_canonical, complete_ml_pipeline
from .jobs_ingestion import format_data_job, ingest_to_staging_job
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

# Create a unique experiment name for each environment to avoid conflicts
hostname = socket.gethostname()
today = datetime.datetime.now().strftime("%Y%m%d")
experiment_name = f"data_pipeline_monitoring_{hostname}_{today}"

# MLFlow monitoring resource
mlflow_monitoring = MLFlowMonitoringResource(
    mlflow_tracking_uri=os.getenv(
        "MLFLOW_TRACKING_URI",
        "http://localhost:8002"  # MLFlow server running on port 8002
    ),
    experiment_name=experiment_name
)

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
)

# Definitions ----------------------------------------------------------------
defs = Definitions(
    assets=[
        dbt_project_assets,
        train_student_dropout_model,
        predict_student_dropout_risk,
        format_history_data,
        format_students,
        format_percentage,
        ingest_to_staging,
    ],
    resources={
        "dbt": dbt_resource,
        "postgres": pg_resource,
        "mlflow": mlflow_resource,
        "mlflow_monitoring": mlflow_monitoring,
    },
    jobs=[
        refresh_canonical, 
        complete_ml_pipeline, 
        format_data_job,
        ingest_to_staging_job,
    ],
    sensors=[new_period_sensor],
)