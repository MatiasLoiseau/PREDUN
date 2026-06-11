"""
Asset Dagster de detección de drift para PREDUN.

Compara la distribución de features del conjunto de entrenamiento (referencia fija,
academic_period <= TRAIN_CUTOFF) contra los datos nuevos (post-cutoff), calculando
PSI por feature. Los resultados se persisten en:

  - MLflow  : métricas PSI por feature en experimento 'drift_monitoring'
  - PostgreSQL : tabla predictions.drift_metrics (consumible por Superset)

El nivel de drift global orienta la decisión MLOps:
  none     → reentrenar rutinariamente con misma arquitectura
  moderate → aumentar frecuencia de reentrenamiento, monitorear segmentos
  high     → revisar features y evaluar arquitectura alternativa
"""
import os
import time
from datetime import datetime

import mlflow
import pandas as pd
from dagster import AssetExecutionContext, asset
from sqlalchemy import create_engine, text

from ..drift_utils import (
    TRAIN_CUTOFF,
    FEATURES_NUM,
    FEATURES_CAT,
    compute_feature_drift,
    drift_summary,
)

# promo_rate_period y promo_rate_win3 ya están incluidas en FEATURES_NUM de drift_utils
ALL_FEATURES_NUM = FEATURES_NUM

DRIFT_EXPERIMENT = "drift_monitoring"

CREATE_DRIFT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS predictions.drift_metrics (
    cycle_period      VARCHAR(20)  NOT NULL,
    reference_period  VARCHAR(20)  NOT NULL,
    feature_name      VARCHAR(100) NOT NULL,
    feature_type      VARCHAR(20),
    psi_value         FLOAT,
    drift_level       VARCHAR(20),
    mean_reference    FLOAT,
    mean_current      FLOAT,
    n_reference       INTEGER,
    n_current         INTEGER,
    computed_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (cycle_period, reference_period, feature_name)
)
"""


def _load_panel(engine) -> pd.DataFrame:
    """Carga las filas en riesgo de student_panel y calcula las features derivadas.

    Se restringe a at_risk = 1 para que la comparación de drift opere sobre la
    misma población que el entrenamiento y el scoring (estudiantes no abandonados
    al período t). Las filas con etiqueta censurada (dropout_next NULL) se incluyen:
    sus features cuentan para el drift de covariables y el PSI del label descarta
    los NULL internamente.
    """
    NUM_COLS = [
        "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
        "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_actividad",
    ]
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1",
        engine,
    )
    df = df.drop_duplicates()
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    import numpy as np
    df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(0, np.nan)
    df["promo_rate_win3"]   = df["promo_win3"]       / df["materias_win3"].replace(0, np.nan)
    return df


def _write_drift_to_db(engine, df_drift: pd.DataFrame, cycle_period: str, reference_period: str):
    """Persiste resultados de drift en predictions.drift_metrics con upsert."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text(CREATE_DRIFT_TABLE_SQL))
        conn.commit()

    rows = df_drift.copy()
    rows["cycle_period"]     = cycle_period
    rows["reference_period"] = reference_period
    rows["computed_at"]      = datetime.utcnow()

    # Upsert: eliminar registros anteriores del mismo ciclo/referencia antes de insertar
    with engine.connect() as conn:
        conn.execute(
            text(
                "DELETE FROM predictions.drift_metrics "
                "WHERE cycle_period = :cp AND reference_period = :rp"
            ),
            {"cp": cycle_period, "rp": reference_period},
        )
        conn.commit()

    rows.to_sql(
        "drift_metrics",
        engine,
        schema="predictions",
        if_exists="append",
        index=False,
        method="multi",
    )


@asset(
    required_resource_keys={"mlflow_monitoring"},
    deps=["dbt_project_assets"],
    description=(
        "Detecta drift entre el conjunto de entrenamiento (referencia) y los datos "
        "nuevos usando PSI. Registra métricas en MLflow y en predictions.drift_metrics."
    ),
)
def detect_data_drift(context: AssetExecutionContext) -> dict:
    """
    Calcula PSI por feature comparando:
      - Referencia : student_panel donde academic_period <= TRAIN_CUTOFF (2022_2C)
      - Actual     : student_panel donde academic_period >  TRAIN_CUTOFF

    La comparación referencia-vs-actual responde la pregunta operacional clave:
    '¿la población que estamos scorando hoy difiere de la que entrenamos el modelo?'
    """
    pg_uri = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
    engine = create_engine(pg_uri)

    # ── 1. Cargar datos ───────────────────────────────────────────────────────
    context.log.info("Cargando student_panel para análisis de drift...")
    start_load = time.time()
    df = _load_panel(engine)
    context.log.info(f"Panel cargado: {len(df):,} filas en {time.time()-start_load:.1f}s")

    ref_mask = df["academic_period"] <= TRAIN_CUTOFF
    cur_mask = df["academic_period"] >  TRAIN_CUTOFF

    df_ref = df[ref_mask].copy()
    df_cur = df[cur_mask].copy()

    cycle_period     = str(df_cur["academic_period"].max())  # período más reciente disponible
    reference_period = TRAIN_CUTOFF

    context.log.info(
        f"Referencia: {len(df_ref):,} filas (hasta {reference_period}) | "
        f"Actual: {len(df_cur):,} filas (desde {TRAIN_CUTOFF} hasta {cycle_period})"
    )

    # ── 2. Calcular PSI ───────────────────────────────────────────────────────
    context.log.info("Calculando PSI por feature...")
    df_drift = compute_feature_drift(
        df_reference=df_ref,
        df_current=df_cur,
        features_num=ALL_FEATURES_NUM,
        features_cat=FEATURES_CAT,
        include_label=True,
    )

    summary = drift_summary(df_drift)
    context.log.info(
        f"Drift global: {summary['overall_level'].upper()} "
        f"(max PSI={summary['max_psi']:.4f}, "
        f"high={summary['n_features_high']}, "
        f"moderate={summary['n_features_mod']}, "
        f"none={summary['n_features_none']})"
    )

    # Log por feature al contexto de Dagster
    for _, row in df_drift.iterrows():
        context.log.info(
            f"  PSI {row['feature_name']:<30} = {row['psi_value']:.4f}  [{row['drift_level']}]"
        )

    # ── 3. Registrar en MLflow ────────────────────────────────────────────────
    with context.resources.mlflow_monitoring.track_job(
        job_name="detect_data_drift",
        tags={
            "cycle_period":     cycle_period,
            "reference_period": reference_period,
            "drift_level":      summary["overall_level"],
        },
    ):
        # Métricas PSI por feature
        for _, row in df_drift.iterrows():
            safe_name = row["feature_name"].replace(" ", "_")
            mlflow.log_metric(f"psi_{safe_name}", row["psi_value"])

        # Métricas de resumen
        mlflow.log_metric("psi_max",             summary["max_psi"])
        mlflow.log_metric("n_features_high_drift",   summary["n_features_high"])
        mlflow.log_metric("n_features_mod_drift",    summary["n_features_mod"])
        mlflow.log_metric("n_reference",         len(df_ref))
        mlflow.log_metric("n_current",           len(df_cur))

        # Guardar tabla de drift como artefacto CSV
        import tempfile, json
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, prefix="drift_report_"
        ) as f:
            df_drift.to_csv(f.name, index=False)
            csv_path = f.name
        mlflow.log_artifact(csv_path, "drift_artifacts")

        # Guardar summary como artefacto JSON
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="drift_summary_"
        ) as f:
            json.dump({**summary, "cycle_period": cycle_period, "reference_period": reference_period}, f, indent=2)
            json_path = f.name
        mlflow.log_artifact(json_path, "drift_artifacts")

    # ── 4. Persistir en PostgreSQL para Superset ──────────────────────────────
    context.log.info("Guardando resultados en predictions.drift_metrics...")
    _write_drift_to_db(engine, df_drift, cycle_period, reference_period)
    context.log.info(f"Guardados {len(df_drift)} registros de drift en PostgreSQL")

    return {
        "cycle_period":      cycle_period,
        "reference_period":  reference_period,
        "overall_drift":     summary["overall_level"],
        "max_psi":           summary["max_psi"],
        "n_features_high":   summary["n_features_high"],
        "n_features_mod":    summary["n_features_mod"],
        "top3_features":     summary["top3_features"],
        "n_reference":       len(df_ref),
        "n_current":         len(df_cur),
    }
