import os
import joblib
import numpy as np
import pandas as pd
import mlflow
from sqlalchemy import text
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score

from dagster import (
    asset,
    AssetKey,
    Output,
    MetadataValue,
    get_dagster_logger,
)

from ..resources import PostgresResource, MLflowResource
from ..constants import PG_URI_ENV, DEFAULT_CHUNK_SIZE

# ----------------------------------------------------------------------
# Asset dbt que produce el panel
PANEL_ASSET_KEY = AssetKey(["dbt", "marts", "student_panel"])

# Columnas
NUM_COLS = [
    "materias_en_periodo",
    "promo_en_periodo",
    "nota_media_en_periodo",
    "materias_win3",
    "promo_win3",
    "nota_win3",
    "dias_desde_ult_periodo",
]
CAT_COLS = ["cod_carrera"]


# ----------------------------------------------------------------------
@asset(
    deps=[PANEL_ASSET_KEY],
    required_resource_keys={"postgres", "mlflow"},
)
def train_student_dropout_model(context):
    """
    Entrena un Pipeline (pre-procesamiento + GradientBoosting) y lo registra
    en MLflow.  El split es temporal: todo ≤ '2022_2C' se usa para entrenar.
    """
    log = get_dagster_logger()
    pg: PostgresResource = context.resources.postgres
    mlflow_res: MLflowResource = context.resources.mlflow

    # -------------------------- carga ---------------------------------
    with pg.get_conn() as conn:
        df_iter = pd.read_sql(
            text(
                """
                SELECT *
                FROM marts.student_panel
                WHERE dropout_next IS NOT NULL
                """
            ),
            conn,
            chunksize=DEFAULT_CHUNK_SIZE,
        )
        df = pd.concat(df_iter, ignore_index=True)

    # -------------------- limpieza + features --------------------------
    df = df.drop_duplicates()

    # convertir numéricas a float
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")

    # ratios de promoción (maneja div/0)
    df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(
        0, np.nan
    )
    df["promo_rate_win3"] = df["promo_win3"] / df["materias_win3"].replace(
        0, np.nan
    )

    # materias acumuladas por alumno-carrera
    df["materias_cum"] = (
        df.sort_values("academic_period")
        .groupby(["legajo", "cod_carrera"])["materias_en_periodo"]
        .cumsum()
    )

    FEATURES_NUM = NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"]
    FEATURES_CAT = CAT_COLS
    X = df[FEATURES_NUM + FEATURES_CAT]
    y = df["dropout_next"].astype(int)

    # split temporal
    train_mask = df["academic_period"] <= "2022_2C"
    X_train, y_train = X[train_mask], y[train_mask]
    X_val, y_val = X[~train_mask], y[~train_mask]

    # ------------------------- pipeline --------------------------------
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocess = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, FEATURES_NUM),
            ("cat", categorical_pipe, FEATURES_CAT),
        ]
    )

    clf = GradientBoostingClassifier(random_state=42)

    pipeline = Pipeline(
        steps=[
            ("prep", preprocess),
            ("model", clf),
        ]
    )

    # --------------------- entrenamiento & métricas --------------------
    with mlflow.start_run(run_name="gb_dropout"):

        pipeline.fit(X_train, y_train)

        preds = pipeline.predict(X_val)
        proba = pipeline.predict_proba(X_val)[:, 1]
        roc = roc_auc_score(y_val, proba)

        # logs MLflow
        mlflow.log_metric("roc_auc", roc)
        mlflow.sklearn.log_model(pipeline, artifact_path="model")

        # logging extra (classification report en artefacto txt)
        report = classification_report(y_val, preds, digits=3)
        mlflow.log_text(report, "clf_report.txt")

        context.add_output_metadata(
            metadata={
                "rows_used": len(df),
                "roc_auc": MetadataValue.float(roc),
                "mlflow_run": MetadataValue.url(
                    mlflow.get_tracking_uri().replace("file:", "file://")
                    + f"/#/experiments/0/runs/{mlflow.active_run().info.run_id}"
                ),
            }
        )

    log.info("🏆 ROC-AUC %.4f", roc)
    return Output(None)


# ----------------------------------------------------------------------
@asset(
    deps=[train_student_dropout_model, PANEL_ASSET_KEY],
    required_resource_keys={"postgres"},
)
def score_student_dropout_risk(context):
    """
    Escorea el período más reciente usando el último modelo en Producción
    (o el último run si no hay stage) y guarda results en predictions.student_risk.
    """
    pg: PostgresResource = context.resources.postgres
    client = mlflow.tracking.MlflowClient()

    # -------------------- selecciona modelo ----------------------------
    try:
        prod = client.get_latest_versions("gb_dropout", stages=["Production"])[0]
        model_uri = f"models:/{prod.name}/{prod.version}"
    except IndexError:
        # fallback: último run del experimento 0
        model_uri = (
            mlflow.search_runs(order_by=["start_time DESC"])
            .iloc[0]["artifact_uri"]
            + "/model"
        )

    pipeline = mlflow.sklearn.load_model(model_uri)

    # -------------------- datos live + features ------------------------
    with pg.get_conn() as conn:
        period = pd.read_sql(
            text("SELECT max(academic_period) AS p FROM marts.student_panel"), conn
        )["p"].iloc[0]

        df_live = pd.read_sql(
            text(
                f"""
                SELECT *
                FROM marts.student_panel
                WHERE academic_period = :p
                """
            ),
            conn,
            params={"p": period},
        )

    # mismo feature-engineering que en entrenamiento
    df_live = df_live.drop_duplicates()
    df_live[NUM_COLS] = df_live[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    df_live["promo_rate_period"] = (
        df_live["promo_en_periodo"] / df_live["materias_en_periodo"].replace(0, np.nan)
    )
    df_live["promo_rate_win3"] = (
        df_live["promo_win3"] / df_live["materias_win3"].replace(0, np.nan)
    )
    df_live["materias_cum"] = (
        df_live.sort_values("academic_period")
        .groupby(["legajo", "cod_carrera"])["materias_en_periodo"]
        .cumsum()
    )

    X_live = df_live[NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"] + CAT_COLS]
    scores = pipeline.predict_proba(X_live)[:, 1]

    results = (
        df_live[["legajo", "cod_carrera"]]
        .assign(academic_period=period, dropout_score=scores)
    )

    # ----------------------- guarda a Postgres -------------------------
    with pg.get_conn() as conn:
        results.to_sql(
            "student_risk",
            conn,
            schema="predictions",
            if_exists="replace",
            index=False,
        )

    context.add_output_metadata(
        {
            "period_scored": period,
            "rows_scored": len(results),
        }
    )

    return Output(None)