"""
Taggea el último run de MLflow con la versión de datos utilizada.
Llamar inmediatamente después de cada ejecución exitosa de complete_ml_pipeline en Dagster.

Uso:
    conda run -n eda-predun python scripts/tag_mlflow_run.py --version 2024_2C
    conda run -n eda-predun python scripts/tag_mlflow_run.py --version 2025_1C
    conda run -n eda-predun python scripts/tag_mlflow_run.py --version 2025_2C
"""

import argparse
import sys
from mlflow.tracking import MlflowClient
import mlflow

MLFLOW_URI = "http://localhost:8002"
EXPERIMENT_NAME = "student_dropout_prediction"

# Qué períodos de staging están cargados en cada versión
PERIODS_IN_VERSION = {
    "2024_2C": ["2024_2C"],
    "2025_1C": ["2024_2C", "2025_1C"],
    "2025_2C": ["2024_2C", "2025_1C", "2025_2C"],
}

# Descripción humana de cada versión para los plots
VERSION_LABELS = {
    "2024_2C": "v1 — datos hasta 2024-2C",
    "2025_1C": "v2 — datos hasta 2025-1C",
    "2025_2C": "v3 — datos hasta 2025-2C",
}


def tag_latest_run(version: str, run_index: int = 0) -> str:
    """
    Taggea el run más reciente del experimento student_dropout_prediction.
    Devuelve el run_id taggeado.
    """
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient()

    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        print(f"ERROR: experimento '{EXPERIMENT_NAME}' no encontrado en {MLFLOW_URI}")
        sys.exit(1)

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["start_time DESC"],
        max_results=run_index + 1,
        filter_string="tags.thesis_run != 'true'",  # solo runs sin taggear
    )

    if not runs:
        # Si no hay runs sin taggear, igual tomar el más reciente (puede ser re-run)
        runs = client.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["start_time DESC"],
            max_results=1,
        )
        if not runs:
            print("ERROR: no se encontraron runs en el experimento")
            sys.exit(1)

    run = runs[run_index] if run_index < len(runs) else runs[0]
    run_id = run.info.run_id

    periods = PERIODS_IN_VERSION[version]

    client.set_tag(run_id, "data_version", version)
    client.set_tag(run_id, "staging_periods", ",".join(periods))
    client.set_tag(run_id, "n_staging_periods", str(len(periods)))
    client.set_tag(run_id, "version_label", VERSION_LABELS[version])
    client.set_tag(run_id, "thesis_run", "true")

    # Buscar modelo registrado asociado a este run
    model_versions = client.search_model_versions("name='student_dropout_model'")
    linked_versions = [mv for mv in model_versions if mv.run_id == run_id]
    if linked_versions:
        mv = linked_versions[0]
        client.set_model_version_tag(
            name="student_dropout_model",
            version=mv.version,
            key="data_version",
            value=version,
        )
        client.set_model_version_tag(
            name="student_dropout_model",
            version=mv.version,
            key="thesis_run",
            value="true",
        )
        print(f"  Modelo registrado: student_dropout_model v{mv.version}")

    # Resumen
    start = run.info.start_time
    duration = (run.info.end_time - run.info.start_time) / 1000 if run.info.end_time else None
    roc_auc = run.data.metrics.get("roc_auc", "N/A")
    accuracy = run.data.metrics.get("validation_accuracy", "N/A")

    print(f"\n{'='*55}")
    print(f"  Run taggeado exitosamente")
    print(f"{'='*55}")
    print(f"  run_id       : {run_id}")
    print(f"  data_version : {version}")
    print(f"  label        : {VERSION_LABELS[version]}")
    print(f"  períodos     : {', '.join(periods)}")
    print(f"  ROC-AUC      : {roc_auc if isinstance(roc_auc, str) else f'{roc_auc:.4f}'}")
    print(f"  Accuracy     : {accuracy if isinstance(accuracy, str) else f'{accuracy:.4f}'}")
    if duration:
        print(f"  Duración     : {duration:.1f}s")
    print(f"{'='*55}\n")

    return run_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Taggea el último run de MLflow con la versión de datos"
    )
    parser.add_argument(
        "--version",
        required=True,
        choices=["2024_2C", "2025_1C", "2025_2C"],
        help="Versión de datos con la que fue entrenado el modelo",
    )
    parser.add_argument(
        "--run-index",
        type=int,
        default=0,
        help="Índice del run a taggear (0=más reciente)",
    )
    args = parser.parse_args()
    tag_latest_run(args.version, args.run_index)
