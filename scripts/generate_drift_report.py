"""
Genera reportes visuales del análisis de deriva de datos (PSI) por ciclo.
Requiere que el activo detect_data_drift haya sido ejecutado para el ciclo indicado.

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_drift_report.py --version 2025_2C
    conda run -n eda-predun python scripts/generate_drift_report.py  # todos los ciclos disponibles

Figuras generadas (en THESIS_FIGS_DIR):
    drift_psi_{version}.png        — barras de PSI por feature para el ciclo indicado
    drift_psi_evolution.png        — evolución del PSI máximo por ciclo (si hay múltiples)
    drift_metrics_{version}.json   — métricas de deriva en JSON para uso posterior
"""

import argparse
import json
import os
import warnings

import matplotlib.patches as mpatches
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

# Paleta
C_HIGH   = "#cb181d"   # rojo — deriva alta
C_MOD    = "#fe9929"   # naranja — deriva moderada
C_NONE   = "#2d6a2d"   # verde — sin deriva
C_BLUE   = "#1a3a6b"
C_GRAY   = "#636363"
C_LGRAY  = "#d9d9d9"

DRIFT_COLORS = {"high": C_HIGH, "moderate": C_MOD, "none": C_NONE}

PSI_THRESHOLDS = {"high": 0.25, "moderate": 0.10}

FEATURE_LABELS = {
    "promo_rate_win3":          "Tasa promoción (ventana 4p)",
    "promo_rate_period":        "Tasa promoción (período)",
    "materias_win3":            "Materias cursadas (ventana 4p)",
    "materias_en_periodo":      "Materias cursadas (período)",
    "promo_en_periodo":         "Materias promocionadas (período)",
    "nota_media_en_periodo":    "Nota media (período)",
    "nota_win3":                "Nota media (ventana 4p)",
    "promo_win3":               "Materias promocionadas (ventana 4p)",
    "dias_desde_ult_actividad": "Días desde últ. actividad",
    "cod_carrera":              "Carrera (cod_carrera)",
    "dropout_next":             "Variable objetivo (dropout_next)",
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


def load_drift_metrics(engine, version: str | None = None) -> pd.DataFrame:
    if version:
        df = pd.read_sql(
            "SELECT * FROM predictions.drift_metrics WHERE cycle_period = %(v)s ORDER BY psi_value DESC",
            engine, params={"v": version},
        )
    else:
        df = pd.read_sql(
            "SELECT * FROM predictions.drift_metrics ORDER BY cycle_period, psi_value DESC",
            engine,
        )
    if df.empty:
        return df
    df["feature_label"] = df["feature_name"].map(lambda x: FEATURE_LABELS.get(x, x))
    df["color"] = df["drift_level"].map(lambda x: DRIFT_COLORS.get(x, C_GRAY))
    return df


# ── Plot 1: Barras PSI por feature para un ciclo ──────────────────────────────
def plot_psi_bars(df: pd.DataFrame, version: str, out_dir: str):
    df_sorted = df.sort_values("psi_value", ascending=True).copy()
    label = VERSION_LABELS.get(version, version)

    fig, ax = plt.subplots(figsize=(8, max(4, len(df_sorted) * 0.52)))

    bars = ax.barh(
        range(len(df_sorted)), df_sorted["psi_value"],
        color=df_sorted["color"].tolist(),
        edgecolor="white", height=0.65, zorder=3,
    )

    # Anotaciones con valor PSI
    for bar, val in zip(bars, df_sorted["psi_value"]):
        ax.text(
            val + 0.005, bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}", va="center", ha="left", fontsize=9,
        )

    # Líneas de umbral
    ax.axvline(PSI_THRESHOLDS["moderate"], color=C_MOD, lw=1.5, ls="--",
               alpha=0.8, zorder=4, label=f"Umbral moderado ({PSI_THRESHOLDS['moderate']})")
    ax.axvline(PSI_THRESHOLDS["high"], color=C_HIGH, lw=1.5, ls="--",
               alpha=0.8, zorder=4, label=f"Umbral alto ({PSI_THRESHOLDS['high']})")

    ax.set_yticks(range(len(df_sorted)))
    ax.set_yticklabels(df_sorted["feature_label"].tolist(), fontsize=9)
    ax.set_xlabel("Population Stability Index (PSI)")
    ax.set_title(
        f"Deriva de datos por feature — {label}\n"
        f"Referencia: entrenamiento (≤ 2022-2C)  |  Actual: validación post-corte",
        fontsize=11,
    )
    ax.set_xlim(0, max(df_sorted["psi_value"].max() * 1.18, 0.35))

    # Leyenda de niveles
    patches = [
        mpatches.Patch(color=C_HIGH,  label="Deriva alta  (PSI ≥ 0.25)"),
        mpatches.Patch(color=C_MOD,   label="Deriva moderada  (0.10 ≤ PSI < 0.25)"),
        mpatches.Patch(color=C_NONE,  label="Sin deriva  (PSI < 0.10)"),
    ]
    ax.legend(handles=patches, loc="lower right", fontsize=8)
    despine(ax)

    fig.tight_layout()
    path = os.path.join(out_dir, f"drift_psi_{version}.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 2: Tabla visual de métricas de deriva ────────────────────────────────
def plot_drift_table(df: pd.DataFrame, version: str, out_dir: str):
    df_sorted = df.sort_values("psi_value", ascending=False).copy()
    label = VERSION_LABELS.get(version, version)

    cols = ["feature_label", "psi_value", "drift_level", "mean_reference", "mean_current"]
    col_headers = ["Feature", "PSI", "Nivel", "Media ref.", "Media actual"]

    rows = []
    for _, r in df_sorted.iterrows():
        mean_ref = f"{r['mean_reference']:.3f}" if pd.notna(r.get("mean_reference")) else "—"
        mean_cur = f"{r['mean_current']:.3f}" if pd.notna(r.get("mean_current")) else "—"
        rows.append([r["feature_label"], f"{r['psi_value']:.4f}", r["drift_level"], mean_ref, mean_cur])

    fig, ax = plt.subplots(figsize=(12, max(3, len(rows) * 0.55 + 1)))
    ax.axis("off")

    table = ax.table(
        cellText=rows,
        colLabels=col_headers,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9.5)
    table.scale(1.0, 1.7)

    # Colorear encabezado
    for j in range(len(col_headers)):
        table[0, j].set_facecolor(C_BLUE)
        table[0, j].set_text_props(color="white", fontweight="bold")

    # Colorear filas según nivel de deriva
    for i, row in enumerate(rows):
        level = row[2]
        bg = {"high": "#fcbba1", "moderate": "#fdd0a2", "none": "#c7e9c0"}.get(level, "white")
        for j in range(len(col_headers)):
            table[i + 1, j].set_facecolor(bg)

    ax.set_title(f"Tabla de deriva de datos — {label}", fontsize=12, pad=10)

    path = os.path.join(out_dir, f"drift_table_{version}.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Plot 3: Evolución del PSI máximo entre ciclos ────────────────────────────
def plot_psi_evolution(engine, out_dir: str):
    df = pd.read_sql(
        """
        SELECT cycle_period,
               MAX(psi_value) AS psi_max,
               SUM(CASE WHEN drift_level = 'high'     THEN 1 ELSE 0 END) AS n_high,
               SUM(CASE WHEN drift_level = 'moderate' THEN 1 ELSE 0 END) AS n_moderate,
               SUM(CASE WHEN drift_level = 'none'     THEN 1 ELSE 0 END) AS n_none
        FROM predictions.drift_metrics
        GROUP BY cycle_period
        ORDER BY cycle_period
        """,
        engine,
    )
    if len(df) < 2:
        print("  SKIP drift_psi_evolution: se necesitan al menos 2 ciclos")
        return

    x = np.arange(len(df))
    x_labels = [VERSION_LABELS.get(p, p) for p in df["cycle_period"]]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Panel izquierdo: PSI máximo por ciclo
    ax1 = axes[0]
    colors = [DRIFT_COLORS.get("high" if v >= 0.25 else "moderate" if v >= 0.10 else "none")
              for v in df["psi_max"]]
    bars = ax1.bar(x, df["psi_max"], color=colors, alpha=0.85, edgecolor="white", width=0.55, zorder=3)
    ax1.axhline(0.25, color=C_HIGH, lw=1.4, ls="--", alpha=0.8, label="Umbral alto (0.25)")
    ax1.axhline(0.10, color=C_MOD,  lw=1.4, ls="--", alpha=0.8, label="Umbral moderado (0.10)")
    for bar, val in zip(bars, df["psi_max"]):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                 f"{val:.3f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels, rotation=15, ha="right")
    ax1.set_ylabel("PSI máximo del ciclo")
    ax1.set_title("PSI máximo por ciclo de actualización")
    ax1.legend(fontsize=8)
    despine(ax1)

    # Panel derecho: cantidad de features por nivel
    ax2 = axes[1]
    w = 0.25
    ax2.bar(x - w, df["n_high"],     w, label="Alta",     color=C_HIGH, alpha=0.85, edgecolor="white", zorder=3)
    ax2.bar(x,     df["n_moderate"], w, label="Moderada", color=C_MOD,  alpha=0.85, edgecolor="white", zorder=3)
    ax2.bar(x + w, df["n_none"],     w, label="Ninguna",  color=C_NONE, alpha=0.85, edgecolor="white", zorder=3)
    ax2.set_xticks(x)
    ax2.set_xticklabels(x_labels, rotation=15, ha="right")
    ax2.set_ylabel("Cantidad de features")
    ax2.set_title("Features por nivel de deriva")
    ax2.legend(fontsize=8)
    ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    despine(ax2)

    fig.suptitle("Evolución de la deriva de datos entre ciclos de actualización — PREDUN",
                 fontsize=12, y=1.01)
    fig.tight_layout()

    path = os.path.join(out_dir, "drift_psi_evolution.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Reporte de texto ──────────────────────────────────────────────────────────
def print_drift_report(df: pd.DataFrame, version: str):
    label = VERSION_LABELS.get(version, version)
    sep = "=" * 70
    hr  = "-" * 70

    print(f"\n{sep}")
    print(f"  ANÁLISIS DE DERIVA — {label}")
    print(sep)

    n_high = (df["drift_level"] == "high").sum()
    n_mod  = (df["drift_level"] == "moderate").sum()
    n_none = (df["drift_level"] == "none").sum()
    global_psi = df["psi_value"].max()

    print(f"\n  Resumen global:")
    print(f"    PSI máximo         : {global_psi:.4f}")
    print(f"    Features deriva alta   : {n_high}")
    print(f"    Features deriva moderada: {n_mod}")
    print(f"    Features sin deriva    : {n_none}")

    print(f"\n  Detalle por feature (ordenado por PSI descendente):")
    print(hr)
    print(f"  {'Feature':<38}  {'PSI':>7}  {'Nivel':>10}  {'Media ref':>10}  {'Media act':>10}")
    print(f"  {'-'*38}  {'-'*7}  {'-'*10}  {'-'*10}  {'-'*10}")

    for _, row in df.sort_values("psi_value", ascending=False).iterrows():
        ref = f"{row['mean_reference']:.4f}" if pd.notna(row.get("mean_reference")) else "    —"
        cur = f"{row['mean_current']:.4f}"   if pd.notna(row.get("mean_current"))   else "    —"
        flag = "⚠" if row["drift_level"] == "high" else ("▲" if row["drift_level"] == "moderate" else " ")
        print(f"  {row['feature_label']:<38}  {row['psi_value']:>7.4f}  {row['drift_level']:>10}  {ref:>10}  {cur:>10}  {flag}")

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
            "SELECT DISTINCT cycle_period FROM predictions.drift_metrics ORDER BY cycle_period",
            engine,
        )["cycle_period"].tolist()
        if not cycles:
            print("ERROR: no hay datos en predictions.drift_metrics")
            return
        versions = cycles

    print(f"\n{'='*60}")
    print(f"  Generando reportes de deriva — ciclos: {', '.join(versions)}")
    print(f"{'='*60}\n")

    all_data = {}
    for v in versions:
        print(f"\n{'─'*50}")
        print(f"  Ciclo: {v}")
        print(f"{'─'*50}")

        df = load_drift_metrics(engine, v)
        if df.empty:
            print(f"  ADVERTENCIA: no hay datos de deriva para {v}")
            continue

        all_data[v] = df

        print_drift_report(df, v)
        plot_psi_bars(df, v, THESIS_FIGS_DIR)
        plot_drift_table(df, v, THESIS_FIGS_DIR)

        # Guardar JSON de métricas de deriva
        records = df[["feature_name", "psi_value", "drift_level",
                       "mean_reference", "mean_current",
                       "n_reference", "n_current"]].to_dict(orient="records")
        summary = {
            "version": v,
            "label": VERSION_LABELS.get(v, v),
            "psi_max": float(df["psi_value"].max()),
            "n_high": int((df["drift_level"] == "high").sum()),
            "n_moderate": int((df["drift_level"] == "moderate").sum()),
            "n_none": int((df["drift_level"] == "none").sum()),
            "features": records,
        }
        json_path = os.path.join(THESIS_FIGS_DIR, f"drift_metrics_{v}.json")
        with open(json_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"  JSON guardado: {json_path}")

    # Figura de evolución (solo si hay 2+ ciclos)
    if len(all_data) >= 2:
        print("\n  Generando figura de evolución de deriva...")
        plot_psi_evolution(engine, THESIS_FIGS_DIR)

    print(f"\n{'='*60}")
    print(f"  Listo. Figuras en {THESIS_FIGS_DIR}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", choices=["2024_2C", "2025_1C", "2025_2C"],
                        default=None, help="Ciclo a analizar (default: todos)")
    args = parser.parse_args()
    main(args.version)
