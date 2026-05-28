"""
Genera el reporte completo de evaluación para una versión específica del modelo.
Carga el modelo desde MLflow, evalúa sobre el conjunto de validación (> 2022_2C)
y produce 6 figuras listas para la tesis.

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_model_report.py --version 2024_2C
"""

import argparse
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
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    auc,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

# ── Constantes ────────────────────────────────────────────────────────────────
MLFLOW_URI      = "http://localhost:8002"
EXPERIMENT_NAME = "student_dropout_prediction"
PG_URI          = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
TRAIN_CUTOFF    = "2022_2C"
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter4"
)

VERSION_LABELS = {
    "2024_2C": "v1 — datos hasta 2024-2C",
    "2025_1C": "v2 — datos hasta 2025-1C",
    "2025_2C": "v3 — datos hasta 2025-2C",
}

NUM_COLS = [
    "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
    "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_periodo",
]
FEATURE_COLS_NUM = NUM_COLS + ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURE_COLS_CAT = ["cod_carrera"]

FEATURE_NAMES_ES = {
    "materias_en_periodo":   "Materias cursadas (período)",
    "promo_en_periodo":      "Materias aprobadas (período)",
    "nota_media_en_periodo": "Nota media (período)",
    "materias_win3":         "Materias cursadas (ventana 4p)",
    "promo_win3":            "Materias aprobadas (ventana 4p)",
    "nota_win3":             "Nota media (ventana 4p)",
    "dias_desde_ult_periodo":"Días desde últ. período",
    "promo_rate_period":     "Tasa aprobación (período)",
    "promo_rate_win3":       "Tasa aprobación (ventana 4p)",
    "materias_cum":          "Materias acumuladas",
}

# Paleta
C_BLUE   = "#1a3a6b"
C_ORANGE = "#d95f02"
C_GREEN  = "#2d6a2d"
C_RED    = "#cb181d"
C_GRAY   = "#636363"
C_LGRAY  = "#d9d9d9"


# ── Estilo global ─────────────────────────────────────────────────────────────
def set_style():
    plt.style.use("seaborn-v0_8-white")
    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.titlepad": 10,
        "axes.labelsize": 10,
        "axes.labelpad": 6,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "legend.framealpha": 0.9,
        "legend.edgecolor": C_LGRAY,
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linestyle": "--",
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.15,
    })


def despine(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ── Carga de datos ────────────────────────────────────────────────────────────
def load_validation_data():
    engine = create_engine(PG_URI)
    print("Cargando student_panel...")
    df = pd.read_sql("SELECT * FROM marts.student_panel", engine)
    df = df.drop_duplicates()
    df[NUM_COLS] = df[NUM_COLS].apply(pd.to_numeric, errors="coerce")
    df["promo_rate_period"] = df["promo_en_periodo"] / df["materias_en_periodo"].replace(0, np.nan)
    df["promo_rate_win3"]   = df["promo_win3"]       / df["materias_win3"].replace(0, np.nan)
    df["materias_cum"] = (
        df.sort_values("academic_period")
          .groupby(["legajo", "cod_carrera"])["materias_en_periodo"]
          .cumsum()
    )
    assert df["dropout_next"].isin([0, 1]).all()

    train_mask   = df["academic_period"] <= TRAIN_CUTOFF
    X_train      = df.loc[train_mask,  FEATURE_COLS_NUM + FEATURE_COLS_CAT]
    y_train      = df.loc[train_mask,  "dropout_next"]
    X_val        = df.loc[~train_mask, FEATURE_COLS_NUM + FEATURE_COLS_CAT]
    y_val        = df.loc[~train_mask, "dropout_next"]
    val_periods  = df.loc[~train_mask, "academic_period"]

    print(f"  Train: {len(X_train):,}  |  Val: {len(X_val):,}")
    print(f"  Períodos val: {sorted(val_periods.unique())}")
    return X_train, y_train, X_val, y_val, val_periods


# ── Carga de modelo ───────────────────────────────────────────────────────────
def load_model(version: str):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = MlflowClient()
    exp    = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        print(f"ERROR: experimento '{EXPERIMENT_NAME}' no encontrado"); sys.exit(1)

    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=f"tags.data_version = '{version}'",
        order_by=["start_time DESC"], max_results=1,
    )
    if not runs:
        print(f"ERROR: no hay run taggeado con data_version='{version}'")
        print(f"  Ejecutá: python scripts/tag_mlflow_run.py --version {version}")
        sys.exit(1)

    run   = runs[0]
    mvs   = [mv for mv in client.search_model_versions("name='student_dropout_model'")
             if mv.run_id == run.info.run_id]
    uri   = f"models:/student_dropout_model/{mvs[0].version}" if mvs else f"runs:/{run.info.run_id}/model"
    print(f"  Run: {run.info.run_id}  |  AUC loggeado: {run.data.metrics.get('roc_auc', 'N/A')}")
    model = mlflow.sklearn.load_model(uri)
    print("  Modelo cargado OK")
    return model, run


def get_feature_names(model):
    prep = model.named_steps["prep"]
    ohe  = prep.named_transformers_["cat"].named_steps["encoder"]
    return FEATURE_COLS_NUM + list(ohe.get_feature_names_out(FEATURE_COLS_CAT))


# ── Plot 1: Curva ROC ─────────────────────────────────────────────────────────
def plot_roc(y_val, proba, version, out_dir):
    fpr, tpr, _ = roc_curve(y_val, proba)
    roc_auc     = auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(5.5, 5))
    ax.plot(fpr, tpr, color=C_BLUE, lw=2.2, label=f"Modelo PREDUN  (AUC = {roc_auc:.3f})")
    ax.plot([0, 1], [0, 1], color=C_GRAY, lw=1.2, ls="--", alpha=0.7,
            label="Clasificador aleatorio  (AUC = 0.500)")
    ax.fill_between(fpr, tpr, alpha=0.07, color=C_BLUE)

    ax.set_xlabel("Tasa de Falsos Positivos  (1 − Especificidad)")
    ax.set_ylabel("Tasa de Verdaderos Positivos  (Sensibilidad)")
    ax.set_title(f"Curva ROC — {VERSION_LABELS[version]}")
    ax.legend(loc="lower right")
    ax.set_xlim(-0.01, 1.01); ax.set_ylim(-0.01, 1.01)
    despine(ax)

    fig.tight_layout()
    path = os.path.join(out_dir, f"roc_curve_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")
    return roc_auc


# ── Plot 2: Curva Precisión-Recall ───────────────────────────────────────────
def plot_pr(y_val, proba, version, out_dir):
    precision, recall, _ = precision_recall_curve(y_val, proba)
    ap       = auc(recall, precision)
    baseline = float(y_val.mean())

    fig, ax = plt.subplots(figsize=(5.5, 5))
    ax.plot(recall, precision, color=C_GREEN, lw=2.2,
            label=f"Modelo PREDUN  (AP = {ap:.3f})")
    ax.axhline(baseline, color=C_GRAY, lw=1.2, ls="--", alpha=0.7,
               label=f"Clasificador aleatorio  (AP ≈ {baseline:.3f})")
    ax.fill_between(recall, precision, alpha=0.07, color=C_GREEN)

    ax.set_xlabel("Recall  (Sensibilidad)")
    ax.set_ylabel("Precisión")
    ax.set_title(f"Curva Precisión-Recall — {VERSION_LABELS[version]}")
    ax.legend(loc="upper right")
    ax.set_xlim(-0.01, 1.01); ax.set_ylim(-0.01, 1.01)
    despine(ax)

    fig.tight_layout()
    path = os.path.join(out_dir, f"pr_curve_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")
    return ap


# ── Plot 3: Matriz de confusión ──────────────────────────────────────────────
def plot_confusion(y_val, preds, version, out_dir):
    cm_norm = confusion_matrix(y_val, preds, normalize="true")
    cm_abs  = confusion_matrix(y_val, preds)
    labels  = ["Activo\n(0)", "Abandono\n(1)"]

    fig, ax = plt.subplots(figsize=(5.2, 4.5))
    sns.heatmap(
        cm_norm, annot=False, cmap="Blues",
        xticklabels=labels, yticklabels=labels,
        ax=ax, linewidths=1.5, linecolor="white",
        vmin=0, vmax=1, cbar_kws={"shrink": 0.82},
    )

    # Anotaciones con color adaptativo y counts absolutos
    thresh = 0.5
    for i in range(2):
        for j in range(2):
            val  = cm_norm[i, j]
            n    = cm_abs[i, j]
            col  = "white" if val > thresh else "#1a1a1a"
            ax.text(j + 0.5, i + 0.38, f"{val:.2f}",
                    ha="center", va="center", fontsize=15,
                    fontweight="bold", color=col)
            ax.text(j + 0.5, i + 0.65, f"({n:,})",
                    ha="center", va="center", fontsize=8.5, color=col, alpha=0.85)

    ax.set_xlabel("Predicho", labelpad=8)
    ax.set_ylabel("Real", labelpad=8)
    ax.set_title(f"Matriz de Confusión (normalizada) — {VERSION_LABELS[version]}")

    fig.tight_layout()
    path = os.path.join(out_dir, f"confusion_matrix_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 4: Importancia de features ─────────────────────────────────────────
def plot_feature_importance(model, version, out_dir, top_n=12):
    all_names   = get_feature_names(model)
    importances = model.named_steps["model"].feature_importances_

    # Consolidar carreras (one-hot → suma)
    names, imps, carrera_imp = [], [], 0.0
    for n, imp in zip(all_names, importances):
        if n.startswith("cod_carrera_"):
            carrera_imp += imp
        else:
            names.append(FEATURE_NAMES_ES.get(n, n))
            imps.append(imp)
    names.append("Carrera (efecto fijo one-hot)")
    imps.append(carrera_imp)

    # Ordenar y recortar
    idx    = np.argsort(imps)[-top_n:]
    names  = [names[i] for i in idx]
    imps   = [imps[i] for i in idx]

    # Color secuencial (azul oscuro = más importante)
    norm   = plt.Normalize(min(imps), max(imps))
    cmap   = plt.cm.Blues
    colors = [cmap(0.35 + 0.65 * norm(v)) for v in imps]

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.barh(range(len(names)), imps, color=colors,
                   edgecolor="white", height=0.68, zorder=3)

    for bar, val in zip(bars, imps):
        ax.text(val + 0.002, bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}", va="center", ha="left", fontsize=9)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.set_xlabel("Importancia (ganancia de información media en los árboles)")
    ax.set_title(f"Importancia de Features — {VERSION_LABELS[version]}")
    ax.set_xlim(0, max(imps) * 1.17)
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f"))
    despine(ax)

    fig.tight_layout()
    path = os.path.join(out_dir, f"feature_importance_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 5: Calibración + distribución de probabilidades ────────────────────
def plot_calibration(y_val, proba, version, out_dir, n_bins=10):
    fop, mpv = calibration_curve(y_val, proba, n_bins=n_bins, strategy="uniform")

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))

    # Panel izquierdo: curva de calibración
    ax = axes[0]
    ax.plot(mpv, fop, "o-", color=C_BLUE, lw=2, markersize=6,
            label="Modelo PREDUN", zorder=4)
    ax.plot([0, 1], [0, 1], color=C_GRAY, lw=1.2, ls="--", alpha=0.7,
            label="Calibración perfecta")
    ax.fill_between(mpv, fop, mpv,
                    where=(fop < mpv), alpha=0.12, color=C_RED,
                    label="Sobreestima riesgo")
    ax.fill_between(mpv, fop, mpv,
                    where=(fop > mpv), alpha=0.12, color=C_GREEN,
                    label="Subestima riesgo")
    ax.set_xlabel("Probabilidad media predicha")
    ax.set_ylabel("Fracción de positivos reales")
    ax.set_title("Curva de Calibración", fontsize=11)
    ax.legend(fontsize=8)
    ax.set_xlim(-0.02, 1.02); ax.set_ylim(-0.02, 1.02)
    despine(ax)

    # Panel derecho: histogramas de probabilidad por clase
    ax2 = axes[1]
    ax2.hist(proba[y_val == 0], bins=35, alpha=0.65, color=C_BLUE,
             density=True, label="Activo (clase 0)", edgecolor="white")
    ax2.hist(proba[y_val == 1], bins=35, alpha=0.65, color=C_RED,
             density=True, label="Abandono (clase 1)", edgecolor="white")
    ax2.axvline(0.5, color=C_GRAY, lw=1.2, ls="--", alpha=0.8, label="Umbral 0.5")
    ax2.set_xlabel("Probabilidad de abandono predicha")
    ax2.set_ylabel("Densidad")
    ax2.set_title("Distribución de Probabilidades por Clase", fontsize=11)
    ax2.legend()
    despine(ax2)

    fig.suptitle(f"Calibración del modelo — {VERSION_LABELS[version]}",
                 fontsize=12, y=1.02)
    fig.tight_layout()
    path = os.path.join(out_dir, f"calibration_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 6: Métricas por período de validación ───────────────────────────────
def plot_metrics_by_period(y_val, proba, preds, val_periods, version, out_dir):
    periods = sorted(val_periods.unique())
    rows    = []
    for p in periods:
        mask = (val_periods == p).values
        if mask.sum() < 50:
            continue
        y_p, pr_p, pd_p = y_val.values[mask], proba[mask], preds[mask]
        roc = roc_auc_score(y_p, pr_p) if len(np.unique(y_p)) > 1 else np.nan
        rows.append({
            "period": p.replace("_", "-"),
            "auc": roc,
            "dropout_real":  y_p.mean() * 100,
            "dropout_pred":  pd_p.mean() * 100,
            "n": mask.sum(),
        })

    if not rows:
        return

    df = pd.DataFrame(rows)
    x  = np.arange(len(df))

    fig, axes = plt.subplots(2, 1, figsize=(9, 7), sharex=True,
                             gridspec_kw={"hspace": 0.08})

    # — Panel superior: AUC por período —
    ax1 = axes[0]
    bars = ax1.bar(x, df["auc"], color=C_BLUE, alpha=0.85,
                   edgecolor="white", width=0.6, zorder=3)
    ax1.axhline(0.5, color=C_RED, lw=1.2, ls="--", alpha=0.7, label="Aleatorio (AUC = 0.5)")
    ax1.set_ylabel("ROC-AUC")
    ax1.set_title(f"Métricas del modelo por período de validación — {VERSION_LABELS[version]}")
    ax1.set_ylim(0.4, 1.0)
    ax1.legend(loc="upper right")
    for bar, val in zip(bars, df["auc"]):
        if not np.isnan(val):
            ax1.text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + 0.006,
                     f"{val:.3f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    despine(ax1)

    # — Panel inferior: tasa de abandono real vs predicha —
    ax2  = axes[1]
    w    = 0.28
    b_r  = ax2.bar(x - w / 2, df["dropout_real"], w, label="Real",
                   color=C_RED,    alpha=0.82, edgecolor="white", zorder=3)
    b_p  = ax2.bar(x + w / 2, df["dropout_pred"], w, label="Predicha",
                   color=C_ORANGE, alpha=0.82, edgecolor="white", zorder=3)
    ax2.set_ylabel("Tasa de abandono (%)")
    ax2.set_xlabel("Período académico")
    ax2.set_xticks(x)
    ax2.set_xticklabels(df["period"], rotation=0, fontsize=9)
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax2.legend(loc="upper right")

    # n debajo del eje, no dentro
    for xi, n in zip(x, df["n"]):
        ax2.text(xi, -ax2.get_ylim()[1] * 0.08,
                 f"n={n:,}", ha="center", va="top", fontsize=7.5, color=C_GRAY)

    despine(ax2)
    fig.tight_layout()
    path = os.path.join(out_dir, f"metrics_by_period_{version}.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main(version: str):
    set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)

    print(f"\n{'='*55}")
    print(f"  Reporte — {VERSION_LABELS[version]}")
    print(f"{'='*55}\n")

    print("1. Cargando modelo...")
    model, run = load_model(version)

    print("\n2. Cargando datos de validación...")
    X_train, y_train, X_val, y_val, val_periods = load_validation_data()

    print("\n3. Predicciones...")
    proba  = model.predict_proba(X_val)[:, 1]
    preds  = model.predict(X_val)

    roc_auc  = roc_auc_score(y_val, proba)
    report   = classification_report(y_val, preds, digits=3, output_dict=True)
    accuracy = (preds == y_val).mean()

    print(f"  ROC-AUC  : {roc_auc:.4f}")
    print(f"  Accuracy : {accuracy:.4f}")
    print(f"  F1 macro : {report['macro avg']['f1-score']:.4f}")
    print(f"  F1  (cls 1 abandono): {report['1']['f1-score']:.4f}")
    print(f"  Prec (cls 1): {report['1']['precision']:.4f}")
    print(f"  Rec  (cls 1): {report['1']['recall']:.4f}")

    # Guardar métricas JSON
    metrics = {
        "version": version, "label": VERSION_LABELS[version],
        "run_id": run.info.run_id,
        "roc_auc": roc_auc, "accuracy": accuracy,
        "train_size": len(X_train), "val_size": len(X_val),
        "val_periods": sorted(val_periods.unique()),
        "dropout_rate_val": float(y_val.mean()),
        "predicted_positive_rate": float(preds.mean()),
        "f1_macro": report["macro avg"]["f1-score"],
        "f1_weighted": report["weighted avg"]["f1-score"],
        "precision_0": report["0"]["precision"], "recall_0": report["0"]["recall"],
        "f1_0": report["0"]["f1-score"], "support_0": report["0"]["support"],
        "precision_1": report["1"]["precision"], "recall_1": report["1"]["recall"],
        "f1_1": report["1"]["f1-score"], "support_1": report["1"]["support"],
    }
    with open(os.path.join(THESIS_FIGS_DIR, f"metrics_{version}.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    print("\n4. Generando figuras...")
    plot_roc(y_val, proba, version, THESIS_FIGS_DIR)
    plot_pr(y_val, proba, version, THESIS_FIGS_DIR)
    plot_confusion(y_val, preds, version, THESIS_FIGS_DIR)
    plot_feature_importance(model, version, THESIS_FIGS_DIR)
    plot_calibration(y_val, proba, version, THESIS_FIGS_DIR)
    plot_metrics_by_period(y_val, proba, preds, val_periods, version, THESIS_FIGS_DIR)

    print(f"\n{'='*55}")
    print(f"  ROC-AUC      : {roc_auc:.4f}")
    print(f"  F1 (abandono): {report['1']['f1-score']:.4f}")
    print(f"  Precision    : {report['1']['precision']:.4f}")
    print(f"  Recall       : {report['1']['recall']:.4f}")
    print(f"  Figuras en   : {THESIS_FIGS_DIR}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True, choices=["2024_2C", "2025_1C", "2025_2C"])
    args = parser.parse_args()
    main(args.version)
