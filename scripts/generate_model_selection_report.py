"""
Genera reportes visuales de la comparación de modelos candidatos por ciclo.
Requiere que el activo train_student_dropout_model haya ejecutado la comparación
multi-modelo para el ciclo indicado (tabla predictions.model_evaluations).

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_model_selection_report.py --version 2025_2C
    conda run -n eda-predun python scripts/generate_model_selection_report.py  # todos los ciclos

Figuras generadas (en THESIS_FIGS_DIR):
    model_selection_{version}.png      — comparación de los 3 candidatos para el ciclo
    model_winner_evolution.png         — evolución del modelo ganador y su AUC entre ciclos
    model_selection_{version}.json     — métricas de la comparación en JSON
"""

import argparse
import json
import os
import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter4"
)

C_BLUE   = "#1a3a6b"
C_ORANGE = "#d95f02"
C_GREEN  = "#2d6a2d"
C_RED    = "#cb181d"
C_GRAY   = "#636363"
C_LGRAY  = "#d9d9d9"
C_GOLD   = "#f7a600"   # ganador

MODEL_COLORS = {
    "GradientBoosting":  "#1565C0",
    "RandomForest":      "#2E7D32",
    "LogisticRegression": "#B71C1C",
}
MODEL_LABELS = {
    "GradientBoosting":   "Gradient Boosting",
    "RandomForest":       "Random Forest",
    "LogisticRegression": "Regresión Logística",
}

VERSION_LABELS = {
    "2024_2C": "v1 — ciclo 2024-2C",
    "2025_1C": "v2 — ciclo 2025-1C",
    "2025_2C": "v3 — ciclo 2025-2C",
}


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
        "axes.spines.top": False,
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


def load_model_evaluations(engine, version: str | None = None) -> pd.DataFrame:
    if version:
        df = pd.read_sql(
            "SELECT * FROM predictions.model_evaluations WHERE cycle_period = %(v)s ORDER BY roc_auc DESC",
            engine, params={"v": version},
        )
    else:
        df = pd.read_sql(
            "SELECT * FROM predictions.model_evaluations ORDER BY cycle_period, roc_auc DESC",
            engine,
        )
    return df


# ── Plot 1: Comparación multi-métrica de candidatos ───────────────────────────
def plot_model_comparison(df: pd.DataFrame, version: str, out_dir: str):
    label = VERSION_LABELS.get(version, version)
    winner = df[df["is_selected_model"]]["model_name"].iloc[0] if df["is_selected_model"].any() else None

    metrics = [
        ("roc_auc",            "ROC-AUC"),
        ("ks_statistic",       "KS Statistic ↑"),
        ("f1_dropout",         "F1 (abandono)"),
        ("precision_dropout",  "Precisión (abandono)"),
        ("recall_dropout",     "Recall (abandono)"),
        ("brier_score",        "Brier Score ↓"),
    ]

    n_metrics = len(metrics)
    n_models  = len(df)
    x = np.arange(n_metrics)
    width = 0.25
    offsets = np.linspace(-(n_models - 1) / 2, (n_models - 1) / 2, n_models) * width

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # ── Subplot izquierdo: barras agrupadas por métrica ──
    ax = axes[0]
    for i, (_, row) in enumerate(df.iterrows()):
        mname = row["model_name"]
        values = [row[k] for k, _ in metrics]
        color  = MODEL_COLORS.get(mname, C_GRAY)
        alpha  = 1.0 if mname == winner else 0.6
        edge   = C_GOLD if mname == winner else "white"
        lw     = 2.0  if mname == winner else 0.5
        mlabel = MODEL_LABELS.get(mname, mname)
        if mname == winner:
            mlabel = f"{mlabel} ★ (seleccionado)"

        bars = ax.bar(x + offsets[i], values, width * 0.9,
                      color=color, alpha=alpha,
                      edgecolor=edge, linewidth=lw,
                      label=mlabel, zorder=3)

        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.005,
                    f"{val:.3f}", ha="center", va="bottom", fontsize=7.5,
                    color=color, fontweight="bold" if mname == winner else "normal")

    ax.set_xticks(x)
    ax.set_xticklabels([m_label for _, m_label in metrics], rotation=15, ha="right")
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("Valor de la métrica")
    ax.set_title(f"Comparación de candidatos — {label}")
    ax.legend(loc="upper right", fontsize=8)
    despine(ax)

    # ── Subplot derecho: radar chart simplificado (barras horizontales por modelo) ──
    ax2 = axes[1]
    key_metrics = [
        ("roc_auc",      "ROC-AUC"),
        ("ks_statistic", "KS"),
        ("f1_dropout",   "F1 (abandono)"),
    ]
    y_positions = np.arange(len(df)) * (len(key_metrics) + 1.5)

    for m_idx, (_, row) in enumerate(df.iterrows()):
        mname  = row["model_name"]
        color  = MODEL_COLORS.get(mname, C_GRAY)
        alpha  = 1.0 if mname == winner else 0.6
        ybase  = y_positions[m_idx]

        for k_idx, (k, k_label) in enumerate(key_metrics):
            val = row[k]
            y   = ybase + k_idx
            bar = ax2.barh(y, val, color=color, alpha=alpha, edgecolor="white",
                           height=0.7, zorder=3)
            ax2.text(val + 0.005, y, f"{val:.3f}", va="center", fontsize=9,
                     color=color, fontweight="bold" if mname == winner else "normal")
            ax2.text(-0.01, y, k_label, va="center", ha="right", fontsize=8.5, color=C_GRAY)

        mlabel = MODEL_LABELS.get(mname, mname)
        if mname == winner:
            mlabel = f"★ {mlabel}"
        ax2.text(0.5, ybase + len(key_metrics) - 1 + 0.8, mlabel,
                 ha="center", fontsize=10,
                 color=color, fontweight="bold" if mname == winner else "normal",
                 transform=ax2.get_yaxis_transform())

    ax2.set_xlim(0, 1.15)
    ax2.set_yticks([])
    ax2.set_xlabel("Valor de la métrica")
    ax2.set_title("Métricas clave por arquitectura")

    # Línea de referencia AUC aleatorio
    ax2.axvline(0.5, color=C_RED, lw=1, ls="--", alpha=0.5, label="AUC aleatorio")
    ax2.legend(fontsize=8)
    despine(ax2)

    fig.suptitle(
        f"Selección competitiva de arquitecturas de modelo — {label}",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()

    path = os.path.join(out_dir, f"model_selection_{version}.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 2: Evolución del modelo ganador y su AUC entre ciclos ───────────────
def plot_winner_evolution(engine, out_dir: str):
    df = pd.read_sql(
        """
        SELECT cycle_period, model_name, roc_auc, ks_statistic, f1_dropout,
               training_time_seconds, train_size, val_size
        FROM predictions.model_evaluations
        WHERE is_selected_model = TRUE
        ORDER BY cycle_period
        """,
        engine,
    )
    df_all = pd.read_sql(
        """
        SELECT cycle_period, model_name, roc_auc
        FROM predictions.model_evaluations
        ORDER BY cycle_period, roc_auc DESC
        """,
        engine,
    )

    if len(df) < 2:
        print("  SKIP model_winner_evolution: se necesitan al menos 2 ciclos")
        return

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    cycles  = df["cycle_period"].tolist()
    x_ticks = [VERSION_LABELS.get(c, c) for c in cycles]
    x       = np.arange(len(cycles))

    # Panel 1: AUC del ganador vs ciclo
    ax1 = axes[0]
    winner_colors = [MODEL_COLORS.get(m, C_GRAY) for m in df["model_name"]]
    bars = ax1.bar(x, df["roc_auc"], color=winner_colors, alpha=0.85, edgecolor="white",
                   width=0.55, zorder=3)
    for bar, val, mname in zip(bars, df["roc_auc"], df["model_name"]):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                 f"{val:.4f}\n({MODEL_LABELS.get(mname, mname)[:5]})",
                 ha="center", va="bottom", fontsize=8.5, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_ticks, rotation=15, ha="right")
    ax1.set_ylabel("ROC-AUC")
    ax1.set_ylim(0.5, 1.0)
    ax1.set_title("AUC del modelo ganador por ciclo")
    despine(ax1)

    # Panel 2: Comparación de los 3 modelos por ciclo (líneas)
    ax2 = axes[1]
    for mname in df_all["model_name"].unique():
        mask   = df_all["model_name"] == mname
        sub    = df_all[mask].sort_values("cycle_period")
        sub_x  = [cycles.index(c) for c in sub["cycle_period"] if c in cycles]
        sub_y  = sub[sub["cycle_period"].isin(cycles)]["roc_auc"].tolist()
        color  = MODEL_COLORS.get(mname, C_GRAY)
        mlabel = MODEL_LABELS.get(mname, mname)
        ax2.plot(sub_x, sub_y, "o-", color=color, lw=2, markersize=7, label=mlabel)
        for xi, yi in zip(sub_x, sub_y):
            ax2.text(xi, yi + 0.005, f"{yi:.3f}", ha="center", fontsize=8.5, color=color)

    ax2.set_xticks(range(len(cycles)))
    ax2.set_xticklabels(x_ticks, rotation=15, ha="right")
    ax2.set_ylabel("ROC-AUC")
    ax2.set_ylim(0.5, 1.0)
    ax2.set_title("AUC de los 3 candidatos por ciclo")
    ax2.legend(loc="lower right", fontsize=8)
    despine(ax2)

    # Panel 3: Tiempo de entrenamiento y tamaño del dataset
    ax3 = axes[2]
    ax3b = ax3.twinx()
    train_k = df["train_size"] / 1000
    bars3 = ax3.bar(x, train_k, color=C_BLUE, alpha=0.4, width=0.55, label="N entrenamiento (k)")
    ax3b.plot(x, df["training_time_seconds"], "s-", color=C_ORANGE, lw=2,
              markersize=7, label="Tiempo (s)", zorder=5)
    for xi, t in zip(x, df["training_time_seconds"]):
        ax3b.text(xi, t + 0.5, f"{t:.0f}s", ha="center", fontsize=8.5, color=C_ORANGE)
    ax3.set_xticks(x)
    ax3.set_xticklabels(x_ticks, rotation=15, ha="right")
    ax3.set_ylabel("Tamaño del entrenamiento (miles de filas)", color=C_BLUE)
    ax3b.set_ylabel("Tiempo de entrenamiento (s)", color=C_ORANGE)
    ax3.set_title("Escala de datos y costo de entrenamiento")
    ax3.tick_params(axis="y", colors=C_BLUE)
    ax3b.tick_params(axis="y", colors=C_ORANGE)
    despine(ax3)

    # Leyenda combinada
    lines1, labels1 = ax3.get_legend_handles_labels()
    lines2, labels2 = ax3b.get_legend_handles_labels()
    ax3.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)

    fig.suptitle("Evolución de la selección de modelos entre ciclos — PREDUN",
                 fontsize=12, y=1.01)
    fig.tight_layout()

    path = os.path.join(out_dir, "model_winner_evolution.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Reporte de texto ──────────────────────────────────────────────────────────
def print_model_report(df: pd.DataFrame, version: str):
    label  = VERSION_LABELS.get(version, version)
    sep    = "=" * 70
    hr     = "-" * 70
    winner = df[df["is_selected_model"]]["model_name"].iloc[0] if df["is_selected_model"].any() else "N/A"

    print(f"\n{sep}")
    print(f"  SELECCIÓN DE MODELOS — {label}")
    print(sep)
    print(f"\n  Modelo seleccionado: {MODEL_LABELS.get(winner, winner)}")
    print(f"\n  Comparación de candidatos:")
    print(hr)
    print(f"  {'Modelo':<22}  {'AUC':>6}  {'KS':>6}  {'F1(abd)':>8}  {'Prec':>6}  {'Rec':>6}  {'Brier':>7}  {'t(s)':>5}  Sel")
    print(f"  {'-'*22}  {'-'*6}  {'-'*6}  {'-'*8}  {'-'*6}  {'-'*6}  {'-'*7}  {'-'*5}  ---")

    for _, row in df.sort_values("roc_auc", ascending=False).iterrows():
        sel    = "✓" if row["is_selected_model"] else " "
        mname  = MODEL_LABELS.get(row["model_name"], row["model_name"])[:22]
        print(f"  {mname:<22}  {row['roc_auc']:>6.4f}  {row['ks_statistic']:>6.4f}  "
              f"{row['f1_dropout']:>8.4f}  {row['precision_dropout']:>6.4f}  "
              f"{row['recall_dropout']:>6.4f}  {row['brier_score']:>7.4f}  "
              f"{row['training_time_seconds']:>5.1f}  {sel}")

    print(f"\n{sep}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main(version: str | None = None):
    set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)

    engine = create_engine(PG_URI)

    if version:
        versions = [version]
    else:
        cycles = pd.read_sql(
            "SELECT DISTINCT cycle_period FROM predictions.model_evaluations ORDER BY cycle_period",
            engine,
        )["cycle_period"].tolist()
        if not cycles:
            print("ERROR: no hay datos en predictions.model_evaluations")
            return
        versions = cycles

    print(f"\n{'='*60}")
    print(f"  Generando reportes de selección de modelos — ciclos: {', '.join(versions)}")
    print(f"{'='*60}\n")

    for v in versions:
        print(f"\n{'─'*50}")
        print(f"  Ciclo: {v}")
        print(f"{'─'*50}")

        df = load_model_evaluations(engine, v)
        if df.empty:
            print(f"  ADVERTENCIA: no hay datos de evaluación para {v}")
            continue

        print_model_report(df, v)
        plot_model_comparison(df, v, THESIS_FIGS_DIR)

        # Guardar JSON
        records = df[[
            "model_name", "roc_auc", "brier_score", "ks_statistic",
            "f1_dropout", "precision_dropout", "recall_dropout",
            "training_time_seconds", "train_size", "val_size", "is_selected_model",
        ]].to_dict(orient="records")
        winner_row = df[df["is_selected_model"]]
        summary = {
            "version": v,
            "label": VERSION_LABELS.get(v, v),
            "winner": winner_row["model_name"].iloc[0] if not winner_row.empty else None,
            "winner_auc": float(winner_row["roc_auc"].iloc[0]) if not winner_row.empty else None,
            "winner_ks":  float(winner_row["ks_statistic"].iloc[0]) if not winner_row.empty else None,
            "candidates": records,
        }
        json_path = os.path.join(THESIS_FIGS_DIR, f"model_selection_{v}.json")
        with open(json_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"  JSON guardado: {json_path}")

    # Figura de evolución entre ciclos (solo si hay 2+ ciclos)
    all_df = load_model_evaluations(engine, version=None)
    if all_df["cycle_period"].nunique() >= 2:
        print("\n  Generando figura de evolución de selección de modelos...")
        plot_winner_evolution(engine, THESIS_FIGS_DIR)

    print(f"\n{'='*60}")
    print(f"  Listo. Figuras en {THESIS_FIGS_DIR}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", choices=["2024_2C", "2025_1C", "2025_2C"],
                        default=None, help="Ciclo a analizar (default: todos)")
    args = parser.parse_args()
    main(args.version)
