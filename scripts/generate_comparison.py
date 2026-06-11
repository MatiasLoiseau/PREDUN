"""
Genera figuras de comparación entre las 3 versiones del modelo.
Requiere haber corrido generate_model_report.py para cada versión primero
(necesita los archivos metrics_{version}.json).

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_comparison.py

Figuras generadas (en THESIS_FIGS_DIR):
    roc_curves_comparison.png    — 3 curvas ROC en el mismo eje
    pr_curves_comparison.png     — 3 curvas PR en el mismo eje
    metrics_comparison.png       — barras de AUC / F1 / Precision / Recall
    model_versions_summary.png   — tabla visual con todas las métricas
    model_versions_summary.csv   — CSV para incluir como tabla en la tesis
"""

import json
import os
import sys
import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from mlflow.tracking import MlflowClient
import mlflow
from sklearn.metrics import auc, precision_recall_curve, roc_curve, roc_auc_score
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

# ── Constantes ───────────────────────────────────────────────────────────────
MLFLOW_URI = "http://localhost:8002"
EXPERIMENT_NAME = "student_dropout_prediction"
PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
TRAIN_CUTOFF = "2022_2C"

THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter4"
)

VERSIONS = ["2024_2C", "2025_1C", "2025_2C"]
VERSION_LABELS = {
    "2024_2C": "v1 — 2024-2C",
    "2025_1C": "v2 — 2025-1C",
    "2025_2C": "v3 — 2025-2C",
}
COLORS = {
    "2024_2C": "#1565C0",   # azul
    "2025_1C": "#2E7D32",   # verde
    "2025_2C": "#E65100",   # naranja
}
MARKERS = {"2024_2C": "o", "2025_1C": "s", "2025_2C": "^"}

NUM_COLS = [
    "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
    "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_actividad",
]
FEATURE_COLS_NUM = NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURE_COLS_CAT = ["cod_carrera"]


# ── Estilo ────────────────────────────────────────────────────────────────────
def set_plot_style():
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 11,
        "axes.titlesize": 12,
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.1,
    })


# ── Carga de métricas y datos ─────────────────────────────────────────────────
def load_metrics(version: str) -> dict:
    path = os.path.join(THESIS_FIGS_DIR, f"metrics_{version}.json")
    if not os.path.exists(path):
        print(f"  ADVERTENCIA: no existe {path}")
        print(f"  Ejecutá primero: python scripts/generate_model_report.py --version {version}")
        return None
    with open(path) as f:
        return json.load(f)


def load_validation_data():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1 AND dropout_next IS NOT NULL",
        engine,
    )
    df = df.drop_duplicates()
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)
    df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(0, np.nan)
    df["promo_rate_win3"] = df["promo_win3"] / df["materias_win3"].replace(0, np.nan)
    df["materias_cum"] = (
        df.sort_values("academic_period")
        .groupby(["legajo", "cod_carrera"])["materias_en_periodo"]
        .cumsum()
    )
    val_mask = df["academic_period"] > TRAIN_CUTOFF
    X_val = df.loc[val_mask, FEATURE_COLS_NUM + FEATURE_COLS_CAT]
    y_val = df.loc[val_mask, "dropout_next"]
    return X_val, y_val


def load_model_for_version(version: str):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient()
    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        return None

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=f"tags.data_version = '{version}' and tags.winning_model = 'true'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        return None

    run = runs[0]
    model_versions = client.search_model_versions("name='student_dropout_model'")
    linked = [mv for mv in model_versions if mv.run_id == run.info.run_id]

    if linked:
        model_uri = f"models:/student_dropout_model/{linked[0].version}"
    else:
        model_uri = f"runs:/{run.info.run_id}/model"

    return mlflow.sklearn.load_model(model_uri)


# ── Figura 1: Curvas ROC superpuestas ─────────────────────────────────────────
def plot_roc_comparison(models_data: dict, X_val, y_val, out_dir):
    fig, ax = plt.subplots(figsize=(6, 5.5))

    for version, model in models_data.items():
        if model is None:
            continue
        proba = model.predict_proba(X_val)[:, 1]
        fpr, tpr, _ = roc_curve(y_val, proba)
        roc_auc = auc(fpr, tpr)
        ax.plot(
            fpr, tpr,
            color=COLORS[version], lw=2.0,
            marker=MARKERS[version], markevery=0.1, markersize=5,
            label=f"{VERSION_LABELS[version]}  (AUC = {roc_auc:.3f})",
        )

    ax.plot([0, 1], [0, 1], "k--", lw=1.2, alpha=0.5, label="Aleatorio (AUC = 0.500)")
    ax.set_xlabel("Tasa de Falsos Positivos (1 − Especificidad)")
    ax.set_ylabel("Tasa de Verdaderos Positivos (Sensibilidad)")
    ax.set_title("Comparación de Curvas ROC — modelos v1/v2/v3")
    ax.legend(loc="lower right", framealpha=0.9)
    ax.set_xlim([-0.01, 1.01])
    ax.set_ylim([-0.01, 1.01])

    path = os.path.join(out_dir, "roc_curves_comparison.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Figura 2: Curvas PR superpuestas ─────────────────────────────────────────
def plot_pr_comparison(models_data: dict, X_val, y_val, out_dir):
    fig, ax = plt.subplots(figsize=(6, 5.5))
    baseline = y_val.mean()

    for version, model in models_data.items():
        if model is None:
            continue
        proba = model.predict_proba(X_val)[:, 1]
        precision, recall, _ = precision_recall_curve(y_val, proba)
        ap = auc(recall, precision)
        ax.plot(
            recall, precision,
            color=COLORS[version], lw=2.0,
            marker=MARKERS[version], markevery=0.1, markersize=5,
            label=f"{VERSION_LABELS[version]}  (AP = {ap:.3f})",
        )

    ax.axhline(baseline, color="k", linestyle="--", lw=1.2, alpha=0.5,
               label=f"Aleatorio (AP ≈ {baseline:.3f})")
    ax.set_xlabel("Recall (Sensibilidad)")
    ax.set_ylabel("Precisión")
    ax.set_title("Comparación de Curvas Precisión-Recall — modelos v1/v2/v3")
    ax.legend(loc="upper right", framealpha=0.9)
    ax.set_xlim([-0.01, 1.01])
    ax.set_ylim([-0.01, 1.01])

    path = os.path.join(out_dir, "pr_curves_comparison.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Figura 3: Barras de métricas ──────────────────────────────────────────────
def plot_metrics_bars(all_metrics: list, out_dir):
    versions_available = [m["version"] for m in all_metrics if m is not None]
    if not versions_available:
        print("  Sin datos para plot de métricas")
        return

    metric_groups = {
        "ROC-AUC": "roc_auc",
        "F1 (abandono)": "f1_1",
        "Precisión (abandono)": "precision_1",
        "Recall (abandono)": "recall_1",
        "F1 (activo)": "f1_0",
        "Accuracy": "accuracy",
    }

    fig, axes = plt.subplots(2, 3, figsize=(12, 7))
    axes = axes.flatten()

    for ax, (metric_name, metric_key) in zip(axes, metric_groups.items()):
        values = []
        colors_list = []
        labels = []
        for m in all_metrics:
            if m is None:
                continue
            values.append(m[metric_key])
            colors_list.append(COLORS[m["version"]])
            labels.append(VERSION_LABELS[m["version"]])

        bars = ax.bar(range(len(values)), values, color=colors_list, alpha=0.85,
                      edgecolor="white", width=0.55)
        ax.set_title(metric_name)
        ax.set_xticks(range(len(values)))
        ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
        ax.set_ylim(0, 1.0)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f"))

        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f"{val:.3f}", ha="center", va="bottom", fontsize=9, fontweight="bold")

    fig.suptitle("Comparación de Métricas — Modelos v1/v2/v3 (set validación > 2022-2C)",
                 fontsize=13, y=1.01)
    fig.tight_layout()

    path = os.path.join(out_dir, "metrics_comparison.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Figura 4: Evolución de tamaño de sets ─────────────────────────────────────
def plot_dataset_sizes(all_metrics: list, out_dir):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))

    labels = [m["label"] for m in all_metrics if m]
    train_sizes = [m["train_size"] for m in all_metrics if m]
    val_sizes = [m["val_size"] for m in all_metrics if m]
    dropout_rates = [m["dropout_rate_val"] * 100 for m in all_metrics if m]

    x = range(len(labels))
    colors_list = [COLORS[m["version"]] for m in all_metrics if m]

    # Panel izquierdo: tamaños
    ax1 = axes[0]
    width = 0.35
    b1 = ax1.bar([xi - width / 2 for xi in x], train_sizes, width,
                 label="Entrenamiento", color="#90CAF9", edgecolor="white")
    b2 = ax1.bar([xi + width / 2 for xi in x], val_sizes, width,
                 label="Validación", color="#EF9A9A", edgecolor="white")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels, rotation=15, ha="right")
    ax1.set_ylabel("Número de registros")
    ax1.set_title("Tamaño del panel por versión")
    ax1.legend()
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    for bar in list(b1) + list(b2):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 500,
                 f"{int(bar.get_height()):,}", ha="center", va="bottom", fontsize=8)

    # Panel derecho: tasa de abandono real en validación
    ax2 = axes[1]
    bars = ax2.bar(x, dropout_rates, color=colors_list, alpha=0.85, edgecolor="white")
    ax2.set_xticks(list(x))
    ax2.set_xticklabels(labels, rotation=15, ha="right")
    ax2.set_ylabel("Tasa de abandono real (%)")
    ax2.set_title("Tasa de abandono en set de validación")
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.1f%%"))

    for bar, val in zip(bars, dropout_rates):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                 f"{val:.1f}%", ha="center", va="bottom", fontsize=10, fontweight="bold")

    fig.suptitle("Evolución del panel de datos por versión del modelo", fontsize=12)
    fig.tight_layout()

    path = os.path.join(out_dir, "dataset_sizes.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── CSV de resumen ────────────────────────────────────────────────────────────
def save_summary_csv(all_metrics: list, out_dir):
    rows = []
    for m in all_metrics:
        if m is None:
            continue
        rows.append({
            "Versión": m["version"],
            "Descripción": m["label"],
            "ROC-AUC": f"{m['roc_auc']:.4f}",
            "Accuracy": f"{m['accuracy']:.4f}",
            "F1 macro": f"{m['f1_macro']:.4f}",
            "F1 (abandono)": f"{m['f1_1']:.4f}",
            "Precisión (abandono)": f"{m['precision_1']:.4f}",
            "Recall (abandono)": f"{m['recall_1']:.4f}",
            "F1 (activo)": f"{m['f1_0']:.4f}",
            "Precisión (activo)": f"{m['precision_0']:.4f}",
            "Recall (activo)": f"{m['recall_0']:.4f}",
            "N train": f"{m['train_size']:,}",
            "N val": f"{m['val_size']:,}",
            "Tasa abandono (val)": f"{m['dropout_rate_val']:.3f}",
            "Períodos validación": ", ".join(m["val_periods"]),
        })

    df = pd.DataFrame(rows)
    path = os.path.join(out_dir, "model_versions_summary.csv")
    df.to_csv(path, index=False)
    print(f"  Guardado: {path}")
    print("\n  Tabla resumen:")
    print(df[["Versión", "ROC-AUC", "F1 (abandono)", "Precisión (abandono)",
              "Recall (abandono)", "N train", "N val"]].to_string(index=False))
    return df


# ── Figura visual de tabla de resumen ─────────────────────────────────────────
def plot_summary_table(all_metrics: list, out_dir):
    rows = []
    display_cols = [
        ("ROC-AUC", "roc_auc"), ("Accuracy", "accuracy"),
        ("F1 (abandono)", "f1_1"), ("Prec. (abandono)", "precision_1"),
        ("Recall (abandono)", "recall_1"),
        ("F1 (activo)", "f1_0"), ("Prec. (activo)", "precision_0"),
    ]

    for m in all_metrics:
        if m is None:
            continue
        row = [m["label"]]
        for _, key in display_cols:
            row.append(f"{m[key]:.4f}")
        rows.append(row)

    col_labels = ["Modelo"] + [c[0] for c in display_cols]

    fig, ax = plt.subplots(figsize=(13, 2.5 + len(rows) * 0.5))
    ax.axis("off")

    table = ax.table(
        cellText=rows,
        colLabels=col_labels,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9.5)
    table.scale(1.0, 1.8)

    # Colorear encabezado
    for j in range(len(col_labels)):
        table[0, j].set_facecolor("#1565C0")
        table[0, j].set_text_props(color="white", fontweight="bold")

    # Colorear filas alternadas
    for i, m in enumerate(all_metrics):
        if m is None:
            continue
        bg = COLORS[m["version"]] + "22"  # alpha hex
        for j in range(len(col_labels)):
            table[i + 1, j].set_facecolor(bg)

    # Resaltar mejor valor de cada columna métrica
    numeric_vals = {
        j: [float(rows[i][j]) for i in range(len(rows))]
        for j in range(1, len(col_labels))
    }
    for j, vals in numeric_vals.items():
        best_i = int(np.argmax(vals))
        table[best_i + 1, j].set_text_props(fontweight="bold")

    ax.set_title("Resumen comparativo de versiones del modelo — PREDUN",
                 fontsize=12, pad=10)

    path = os.path.join(out_dir, "model_versions_summary.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    set_plot_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print("  Generando comparación entre modelos v1 / v2 / v3")
    print(f"{'='*60}\n")

    # 1. Cargar métricas pre-calculadas
    print("1. Cargando métricas pre-calculadas...")
    all_metrics = []
    for v in VERSIONS:
        m = load_metrics(v)
        all_metrics.append(m)
        if m:
            print(f"  ✓ {v}: AUC={m['roc_auc']:.4f}, F1(abandon)={m['f1_1']:.4f}")
        else:
            print(f"  ✗ {v}: falta metrics_{v}.json")

    available = [m for m in all_metrics if m is not None]
    if not available:
        print("\nERROR: no hay métricas disponibles. Ejecutá generate_model_report.py primero.")
        sys.exit(1)

    # 2. Cargar datos de validación (necesarios para re-calcular curvas ROC/PR)
    print("\n2. Cargando datos de validación desde PostgreSQL...")
    try:
        X_val, y_val = load_validation_data()
        print(f"  {len(X_val):,} filas de validación cargadas")
    except Exception as e:
        print(f"  ADVERTENCIA: no se pudo cargar datos ({e}). Saltando curvas ROC/PR.")
        X_val, y_val = None, None

    # 3. Cargar modelos (para curvas ROC/PR con datos actuales)
    models_data = {}
    if X_val is not None:
        print("\n3. Cargando modelos desde MLflow...")
        for v in VERSIONS:
            m = load_model_for_version(v)
            models_data[v] = m
            print(f"  {'✓' if m else '✗'} {v}")

    # 4. Generar figuras
    print("\n4. Generando figuras de comparación...")

    if X_val is not None and any(m is not None for m in models_data.values()):
        plot_roc_comparison(models_data, X_val, y_val, THESIS_FIGS_DIR)
        plot_pr_comparison(models_data, X_val, y_val, THESIS_FIGS_DIR)

    plot_metrics_bars(available, THESIS_FIGS_DIR)
    plot_dataset_sizes(available, THESIS_FIGS_DIR)
    plot_summary_table(available, THESIS_FIGS_DIR)
    save_summary_csv(available, THESIS_FIGS_DIR)

    print(f"\n{'='*60}")
    print(f"  Comparación completa. Figuras en:")
    print(f"  {THESIS_FIGS_DIR}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
