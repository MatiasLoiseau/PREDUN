"""
Assets de entrenamiento y scoring para PREDUN.

Cambios respecto a la versión original:
  - train_student_dropout_model entrena 3 modelos candidatos en paralelo:
      GradientBoostingClassifier (baseline original)
      RandomForestClassifier     (ensemble paralelo, más rápido)
      LogisticRegression         (baseline lineal, máxima interpretabilidad)
  - El mejor modelo por AUC se registra en el MLflow Model Registry.
  - Cada candidato queda trazado como run independiente en MLflow con
    el tag model_candidate=True, permitiendo comparar en la UI.
  - Los resultados de la competencia se persisten en
    predictions.model_evaluations para consumo en Superset.
  - predict_student_dropout_risk no cambia: carga el mejor modelo del
    Registry y genera predicciones como antes.
"""
import os
import shutil
import time
import subprocess
import tempfile
import json
import logging

from dagster import asset, AssetExecutionContext


def _conda_bin() -> str:
    return os.environ.get("CONDA_EXE") or shutil.which("conda") or "conda"


# ── Script de entrenamiento multi-modelo ──────────────────────────────────────

def run_multi_model_training_in_conda_env(context: AssetExecutionContext) -> dict:
    """
    Ejecuta el entrenamiento de los 3 modelos candidatos en el entorno eda-predun.

    Retorna un dict con el nombre del modelo ganador, su AUC y el run_id de MLflow.
    """
    pg_uri = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")

    training_script = f'''
import os, time, json, warnings
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, roc_auc_score, brier_score_loss,
    average_precision_score,
)
warnings.filterwarnings("ignore")

# ── Configuración ──────────────────────────────────────────────────────────────
mlflow.set_tracking_uri("http://localhost:8002")
mlflow.set_experiment("student_dropout_prediction")

PG_URI = "{pg_uri}"
engine = create_engine(PG_URI)
TRAIN_CUTOFF = "2022_2C"

NUM_COLS = [
    "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
    "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_periodo",
]

# ── Carga y preparación de datos ───────────────────────────────────────────────
print("Cargando student_panel...")
df = pd.read_sql("SELECT * FROM marts.student_panel", engine)
df = df.drop_duplicates()
df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
assert df["dropout_next"].isin([0, 1]).all(), "Valores inesperados en dropout_next"

df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(0, np.nan)
df["promo_rate_win3"]   = df["promo_win3"]       / df["materias_win3"].replace(0, np.nan)
df["materias_cum"] = (
    df.sort_values("academic_period")
      .groupby(["legajo", "cod_carrera"])["materias_en_periodo"]
      .cumsum()
)

FEATURE_COLS_NUM = NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURE_COLS_CAT = ["cod_carrera"]

X = df[FEATURE_COLS_NUM + FEATURE_COLS_CAT]
y = df["dropout_next"]

train_mask = df["academic_period"] <= TRAIN_CUTOFF
X_train, y_train = X[train_mask], y[train_mask]
X_val,   y_val   = X[~train_mask], y[~train_mask]

# ── Subsampleo del training para desarrollo/pruebas ───────────────────────────
# Con 1.4M filas el entrenamiento es muy lento. Para pruebas usamos el 15%
# estratificado (≈210K filas), suficiente para comparar modelos.
# TODO: quitar TRAIN_SAMPLE_FRAC (ponerlo en 1.0) para el run final de la tesis.
TRAIN_SAMPLE_FRAC = 0.15
if TRAIN_SAMPLE_FRAC < 1.0:
    X_train, _, y_train, _ = train_test_split(
        X_train, y_train,
        train_size=TRAIN_SAMPLE_FRAC,
        stratify=y_train,
        random_state=42,
    )
    print(f"Subsampleo aplicado ({{TRAIN_SAMPLE_FRAC:.0%}}): {{X_train.shape[0]:,}} filas de entrenamiento")

print(f"Train: {{X_train.shape[0]:,}} | Val: {{X_val.shape[0]:,}}")

# ── Preprocessing pipeline (compartido por todos los modelos) ──────────────────
def build_pipeline(classifier):
    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])
    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore")),
    ])
    preprocess = ColumnTransformer([
        ("num", numeric_pipe,      FEATURE_COLS_NUM),
        ("cat", categorical_pipe,  FEATURE_COLS_CAT),
    ])
    return Pipeline([("prep", preprocess), ("model", classifier)])

# ── Modelos candidatos ─────────────────────────────────────────────────────────
# Parámetros reducidos para desarrollo. Para el run final de la tesis,
# aumentar n_estimators a 100/200 y quitar max_depth.
model_candidates = [
    ("GradientBoosting",   GradientBoostingClassifier(
        n_estimators=30, max_depth=3, subsample=0.8, random_state=42)),
    ("RandomForest",       RandomForestClassifier(
        n_estimators=30, max_depth=8, random_state=42, n_jobs=-1)),
    ("LogisticRegression", LogisticRegression(
        max_iter=300, solver="saga", random_state=42, n_jobs=-1)),
]

# ── Entrenamiento y evaluación de cada candidato ───────────────────────────────
results = []
client  = mlflow.tracking.MlflowClient()

for model_name, clf in model_candidates:
    print(f"\\nEntrenando {{model_name}}...")
    t0 = time.time()

    pipeline = build_pipeline(clf)

    with mlflow.start_run(run_name=f"candidate_{{model_name}}_training") as run:
        # Tags que identifican este run como candidato de esta comparación
        mlflow.set_tag("model_candidate",  "true")
        mlflow.set_tag("model_name",       model_name)
        mlflow.set_tag("train_cutoff",     TRAIN_CUTOFF)

        mlflow.log_param("classifier",            model_name)
        mlflow.log_param("train_size",            X_train.shape[0])
        mlflow.log_param("val_size",              X_val.shape[0])
        mlflow.log_param("train_period_cutoff",   TRAIN_CUTOFF)
        mlflow.log_param("num_features",          len(FEATURE_COLS_NUM))
        mlflow.log_param("cat_features",          len(FEATURE_COLS_CAT))

        pipeline.fit(X_train, y_train)
        training_time = time.time() - t0

        preds = pipeline.predict(X_val)
        proba = pipeline.predict_proba(X_val)[:, 1]

        roc_auc   = roc_auc_score(y_val, proba)
        brier     = brier_score_loss(y_val, proba)
        avg_prec  = average_precision_score(y_val, proba)
        report    = classification_report(y_val, preds, digits=3, output_dict=True)
        accuracy  = float((preds == y_val).mean())

        # KS statistic
        proba_pos = np.sort(proba[y_val == 1])
        proba_neg = np.sort(proba[y_val == 0])
        combined  = np.sort(np.concatenate([proba_pos, proba_neg]))
        cdf_pos   = np.searchsorted(proba_pos, combined, side="right") / max(len(proba_pos), 1)
        cdf_neg   = np.searchsorted(proba_neg, combined, side="right") / max(len(proba_neg), 1)
        ks_stat   = float(np.max(np.abs(cdf_pos - cdf_neg)))

        mlflow.log_metric("roc_auc",              roc_auc)
        mlflow.log_metric("brier_score",          brier)
        mlflow.log_metric("average_precision",    avg_prec)
        mlflow.log_metric("ks_statistic",         ks_stat)
        mlflow.log_metric("validation_accuracy",  accuracy)
        mlflow.log_metric("training_time_seconds",training_time)
        mlflow.log_metric("f1_dropout",           report["1"]["f1-score"])
        mlflow.log_metric("precision_dropout",    report["1"]["precision"])
        mlflow.log_metric("recall_dropout",       report["1"]["recall"])

        # Guardar el modelo como artefacto (no registrar en Registry aún)
        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
        )

        results.append({{
            "model_name":            model_name,
            "run_id":                run.info.run_id,
            "roc_auc":               roc_auc,
            "brier_score":           brier,
            "average_precision":     avg_prec,
            "ks_statistic":          ks_stat,
            "f1_dropout":            report["1"]["f1-score"],
            "precision_dropout":     report["1"]["precision"],
            "recall_dropout":        report["1"]["recall"],
            "training_time_seconds": training_time,
            "train_size":            X_train.shape[0],
            "val_size":              X_val.shape[0],
        }})

        print(f"  {{model_name}}: AUC={{roc_auc:.4f}}  Brier={{brier:.4f}}  KS={{ks_stat:.4f}}  t={{training_time:.1f}}s")

# ── Selección del mejor modelo ─────────────────────────────────────────────────
best = max(results, key=lambda r: r["roc_auc"])
print(f"\\nMejor modelo: {{best['model_name']}} (AUC={{best['roc_auc']:.4f}})")

# Registrar SOLO el ganador en el Model Registry
model_uri = f"runs:/{{best['run_id']}}/model"
mv = mlflow.register_model(
    model_uri=model_uri,
    name="student_dropout_model",
)
# ── Tagging automático del run ganador (reemplaza tag_mlflow_run.py) ──────────
# Derivamos staging_periods desde la BD para no hardcodear.
try:
    sp_df = pd.read_sql(
        "SELECT DISTINCT academic_period FROM staging.cursada_historica_raw ORDER BY 1",
        engine,
    )
    staging_periods = sp_df["academic_period"].tolist()
except Exception:
    staging_periods = [cycle_period]

version_label = f"v{{mv.version}} — datos hasta {{cycle_period.replace('_', '-')}}"

client.set_tag(best["run_id"], "winning_model",         "true")
client.set_tag(best["run_id"], "registered_in_registry","true")
client.set_tag(best["run_id"], "registry_version",      str(mv.version))
client.set_tag(best["run_id"], "data_version",          cycle_period)
client.set_tag(best["run_id"], "thesis_run",            "true")
client.set_tag(best["run_id"], "staging_periods",       ",".join(staging_periods))
client.set_tag(best["run_id"], "n_staging_periods",     str(len(staging_periods)))
client.set_tag(best["run_id"], "version_label",         version_label)

client.set_model_version_tag("student_dropout_model", str(mv.version), "data_version", cycle_period)
client.set_model_version_tag("student_dropout_model", str(mv.version), "thesis_run",   "true")

print(f"  Registrado como student_dropout_model v{{mv.version}}")
print(f"  Tags automáticos: data_version={{cycle_period}}, staging_periods={{staging_periods}}")

# ── Persistir comparación en PostgreSQL ───────────────────────────────────────
from datetime import datetime

create_schema_sql = "CREATE SCHEMA IF NOT EXISTS predictions"
create_table_sql  = """
CREATE TABLE IF NOT EXISTS predictions.model_evaluations (
    cycle_period              VARCHAR(20)  NOT NULL,
    model_name                VARCHAR(100) NOT NULL,
    mlflow_run_id             VARCHAR(100),
    roc_auc                   FLOAT,
    brier_score               FLOAT,
    average_precision         FLOAT,
    ks_statistic              FLOAT,
    f1_dropout                FLOAT,
    precision_dropout         FLOAT,
    recall_dropout            FLOAT,
    training_time_seconds     FLOAT,
    train_size                INTEGER,
    val_size                  INTEGER,
    is_selected_model         BOOLEAN,
    evaluated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (cycle_period, model_name)
)
"""

# Usamos el período más reciente del panel como identificador del ciclo
cycle_period = str(df["academic_period"].max())

with engine.connect() as conn:
    conn.execute(text(create_schema_sql))
    conn.execute(text(create_table_sql))
    # Limpiar evaluaciones previas del mismo ciclo para este run
    conn.execute(
        text("DELETE FROM predictions.model_evaluations WHERE cycle_period = :cp"),
        {{"cp": cycle_period}},
    )
    conn.commit()

rows = []
for r in results:
    rows.append({{
        "cycle_period":           cycle_period,
        "model_name":             r["model_name"],
        "mlflow_run_id":          r["run_id"],
        "roc_auc":                r["roc_auc"],
        "brier_score":            r["brier_score"],
        "average_precision":      r["average_precision"],
        "ks_statistic":           r["ks_statistic"],
        "f1_dropout":             r["f1_dropout"],
        "precision_dropout":      r["precision_dropout"],
        "recall_dropout":         r["recall_dropout"],
        "training_time_seconds":  r["training_time_seconds"],
        "train_size":             r["train_size"],
        "val_size":               r["val_size"],
        "is_selected_model":      (r["model_name"] == best["model_name"]),
        "evaluated_at":           datetime.utcnow(),
    }})

pd.DataFrame(rows).to_sql(
    "model_evaluations", engine, schema="predictions",
    if_exists="append", index=False, method="multi",
)
print(f"Comparación guardada en predictions.model_evaluations (ciclo {{cycle_period}})")

# ── Resultado para Dagster ─────────────────────────────────────────────────────
output = {{
    "best_model":       best["model_name"],
    "roc_auc":          best["roc_auc"],
    "brier_score":      best["brier_score"],
    "ks_statistic":     best["ks_statistic"],
    "run_id":           best["run_id"],
    "registry_version": mv.version,
    "cycle_period":     cycle_period,
    "all_models":       [{{
        "name": r["model_name"],
        "roc_auc": round(r["roc_auc"], 4),
        "brier":   round(r["brier_score"], 4),
    }} for r in sorted(results, key=lambda x: x["roc_auc"], reverse=True)],
}}
print("DAGSTER_RESULT:" + json.dumps(output))
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(training_script)
        script_path = f.name

    try:
        context.log.info("Iniciando entrenamiento multi-modelo en eda-predun...")
        t0 = time.time()

        result = subprocess.run(
            [_conda_bin(), "run", "-n", "eda-predun", "python", script_path],
            check=True, capture_output=True, text=True,
        )
        duration = time.time() - t0

        context.log.info(f"Entrenamiento completado en {duration:.1f}s")
        for line in result.stdout.splitlines():
            context.log.info(line)

        for line in result.stdout.strip().split("\n"):
            if line.startswith("DAGSTER_RESULT:"):
                return json.loads(line.replace("DAGSTER_RESULT:", ""))

        return {"status": "completed", "duration": duration}

    except subprocess.CalledProcessError as e:
        context.log.error(f"Error en entrenamiento: {e.stderr}")
        raise
    finally:
        if os.path.exists(script_path):
            os.unlink(script_path)


# ── Asset de entrenamiento ────────────────────────────────────────────────────

@asset(
    required_resource_keys={"mlflow_monitoring"},
    deps=["dbt_project_assets"],
)
def train_student_dropout_model(context: AssetExecutionContext):
    """
    Entrena 3 modelos candidatos (GradientBoosting, RandomForest, LogisticRegression),
    los evalúa sobre el mismo set de validación, registra el mejor en el MLflow
    Model Registry y persiste la comparación en predictions.model_evaluations.
    """
    with context.resources.mlflow_monitoring.track_job(
        job_name="train_student_dropout_model",
        tags={"environment": "eda-predun", "mode": "multi_model"},
    ):
        context.log.info("Iniciando entrenamiento multi-modelo")
        result = run_multi_model_training_in_conda_env(context)

        if "roc_auc" in result:
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name="model_training",
                start_time=time.time() - result.get("training_time", 0),
                success=True,
                additional_metrics={
                    "best_model_auc":     result["roc_auc"],
                    "training_samples":   result.get("train_size", 0),
                    "validation_samples": result.get("val_size", 0),
                },
            )
        context.log.info(
            f"Mejor modelo: {result.get('best_model', '?')} "
            f"(AUC={result.get('roc_auc', '?')})"
        )
        return result


# ── Script de scoring (sin cambios respecto al original) ──────────────────────

def run_prediction_in_conda_env(context: AssetExecutionContext) -> dict:
    """Genera predicciones en eda-predun usando el mejor modelo del MLflow Registry."""
    pg_uri = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")

    prediction_script = f'''
import os, time, json
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine, text

mlflow.set_tracking_uri("http://localhost:8002")
mlflow.set_experiment("student_dropout_prediction")

PG_URI = "{pg_uri}"
engine = create_engine(PG_URI)

print("Cargando último modelo del Registry...")
client = mlflow.tracking.MlflowClient()
try:
    latest = client.get_latest_versions("student_dropout_model", stages=["None"])
    if not latest:
        latest = client.get_latest_versions("student_dropout_model")
    if not latest:
        raise Exception("No hay modelo entrenado en el Registry")
    model_version = latest[0].version
    model = mlflow.sklearn.load_model(f"models:/student_dropout_model/{{model_version}}")
    print(f"Modelo v{{model_version}} cargado OK")
except Exception as e:
    print(f"Error cargando modelo: {{e}}")
    raise

print("Cargando estudiantes activos...")
active_students = pd.read_sql(
    "SELECT DISTINCT legajo FROM marts.student_status WHERE status = \\'estudiando\\'",
    engine,
)
print(f"{{len(active_students)}} estudiantes activos")
if len(active_students) == 0:
    print("Sin estudiantes activos — saliendo")
    exit(0)

legajos_list = "\\', \\'".join(active_students["legajo"].astype(str))
student_panel = pd.read_sql(
    f"SELECT * FROM marts.student_panel WHERE legajo IN (\\\'{{legajos_list}}\\\') ORDER BY legajo, academic_period DESC",
    engine,
)
if len(student_panel) == 0:
    print("Sin datos de panel para activos — saliendo")
    exit(0)

df = student_panel.groupby("legajo").first().reset_index()
print(f"{{len(df)}} registros más recientes para scoring")

NUM_COLS = [
    "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
    "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_periodo",
]
df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(0, np.nan)
df["promo_rate_win3"]   = df["promo_win3"]       / df["materias_win3"].replace(0, np.nan)
df["materias_cum"]      = df["materias_en_periodo"].cumsum()

FEATURE_COLS_NUM = NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURE_COLS_CAT = ["cod_carrera"]
X = df[FEATURE_COLS_NUM + FEATURE_COLS_CAT]

predictions  = model.predict(X)
probabilities = model.predict_proba(X)[:, 1]

print(f"Tasa de abandono predicha: {{predictions.mean():.3f}}")

results_df = pd.DataFrame({{
    "legajo":               df["legajo"],
    "academic_period":      df["academic_period"],
    "cod_carrera":          df["cod_carrera"],
    "dropout_prediction":   predictions,
    "dropout_probability":  probabilities,
    "prediction_date":      pd.Timestamp.now(),
    "model_version":        model_version,
}})

with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS predictions.student_dropout_predictions (
            legajo               VARCHAR(50),
            academic_period      VARCHAR(20),
            cod_carrera          VARCHAR(20),
            dropout_prediction   INTEGER,
            dropout_probability  FLOAT,
            prediction_date      TIMESTAMP,
            model_version        VARCHAR(50),
            PRIMARY KEY (legajo, prediction_date)
        )"""))
    conn.commit()

results_df.to_sql(
    "student_dropout_predictions", engine, schema="predictions",
    if_exists="append", index=False, method="multi",
)
print(f"{{len(results_df)}} predicciones guardadas")

summary = {{
    "total_students":        len(results_df),
    "predicted_dropouts":    int(predictions.sum()),
    "dropout_rate":          float(predictions.mean()),
    "avg_dropout_probability": float(probabilities.mean()),
    "model_version":         model_version,
    "prediction_date":       pd.Timestamp.now().isoformat(),
}}
print("DAGSTER_RESULT:" + json.dumps(summary))
'''

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(prediction_script)
        script_path = f.name

    try:
        context.log.info("Generando predicciones en eda-predun...")
        t0 = time.time()
        result = subprocess.run(
            [_conda_bin(), "run", "-n", "eda-predun", "python", script_path],
            check=True, capture_output=True, text=True,
        )
        context.log.info(f"Scoring completado en {time.time()-t0:.1f}s")
        for line in result.stdout.splitlines():
            context.log.info(line)

        for line in result.stdout.strip().split("\n"):
            if line.startswith("DAGSTER_RESULT:"):
                return json.loads(line.replace("DAGSTER_RESULT:", ""))

        return {"status": "completed"}

    except subprocess.CalledProcessError as e:
        context.log.error(f"Error en scoring: {e.stderr}")
        raise
    finally:
        if os.path.exists(script_path):
            os.unlink(script_path)


@asset(
    required_resource_keys={"mlflow_monitoring"},
    deps=["train_student_dropout_model", "dbt_project_assets"],
)
def predict_student_dropout_risk(context: AssetExecutionContext):
    """
    Genera predicciones de riesgo de abandono para estudiantes activos usando el
    último modelo registrado en el MLflow Model Registry.
    """
    with context.resources.mlflow_monitoring.track_job(
        job_name="predict_student_dropout_risk",
        tags={"environment": "eda-predun"},
    ):
        result = run_prediction_in_conda_env(context)
        if "total_students" in result:
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name="model_prediction",
                start_time=time.time(),
                success=True,
                additional_metrics={
                    "predicted_students": result.get("total_students", 0),
                    "predicted_dropouts": result.get("predicted_dropouts", 0),
                    "dropout_rate":       result.get("dropout_rate", 0),
                },
            )
        return result
