"""
Genera plots exploratorios del dataset de UNDAV directamente desde PostgreSQL.
Puede correrse en cualquier momento después de que las tablas canonical y marts
estén materializadas.

Uso (desde /Users/matiasloiseau/Workspace/PREDUN/):
    conda run -n eda-predun python scripts/generate_data_plots.py
"""

import os
import warnings

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
TRAIN_CUTOFF = "2022_2C"
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter4"
)

# ── Paleta institucional ──────────────────────────────────────────────────────
C_BLUE   = "#1a3a6b"   # azul UNDAV / entrenamiento
C_ORANGE = "#d95f02"   # validación / abandono
C_GREEN  = "#2d6a2d"   # graduados
C_GRAY   = "#636363"
C_LBLUE  = "#6baed6"   # azul claro
C_RED    = "#cb181d"   # abandono / riesgo
C_LGRAY  = "#d9d9d9"

STATUS_COLORS = {"estudiando": C_BLUE, "graduado": C_GREEN, "abandonó": C_RED}
STATUS_LABELS = {"estudiando": "Estudiando", "graduado": "Completó oblig. académicas", "abandonó": "Abandonó"}


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
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.35,
        "grid.linestyle": "--",
        "figure.dpi": 120,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.15,
    })


def despine(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


# ── 1. Distribución de estados estudiantiles ─────────────────────────────────
def plot_student_status(student_status, out_dir):
    order  = ["estudiando", "graduado", "abandonó"]
    counts = student_status["status"].value_counts().reindex(
        [s for s in order if s in student_status["status"].unique()]
    )
    total  = counts.sum()
    labels = [STATUS_LABELS.get(s, s) for s in counts.index]
    colors = [STATUS_COLORS.get(s, C_GRAY) for s in counts.index]
    pcts   = counts.values / total * 100

    fig, ax = plt.subplots(figsize=(7, 3.6))

    bars = ax.barh(labels, counts.values, color=colors, edgecolor="white",
                   height=0.55, zorder=3)

    # Etiqueta dentro/fuera de cada barra (fuera si la barra es corta)
    max_val = counts.max()
    for bar, n, pct in zip(bars, counts.values, pcts):
        y_mid = bar.get_y() + bar.get_height() / 2
        if pct > 10:
            ax.text(bar.get_width() / 2, y_mid,
                    f"{n:,}  ({pct:.1f}%)",
                    va="center", ha="center", fontsize=10,
                    fontweight="bold", color="white")
        else:
            ax.text(bar.get_width() + max_val * 0.015, y_mid,
                    f"{n:,}  ({pct:.1f}%)",
                    va="center", ha="left", fontsize=10,
                    fontweight="bold", color=C_GRAY)

    ax.set_xlabel("Legajos únicos")
    ax.set_title(f"Estado académico de los legajos — UNDAV 2011–2025\n"
                 f"Total: {total:,} legajos únicos", fontsize=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_xlim(0, counts.max() * 1.05)
    despine(ax)
    ax.invert_yaxis()

    fig.tight_layout()
    path = os.path.join(out_dir, "student_status_distribution.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 2. Estudiantes activos y tasa de abandono por período ────────────────────
def plot_active_per_period(student_panel, out_dir):
    stats = (
        student_panel.groupby("academic_period")
        .agg(n_legajos=("legajo", "nunique"), dropout_rate=("dropout_next", "mean"))
        .reset_index()
        .sort_values("academic_period")
    )
    stats["split"] = stats["academic_period"].apply(
        lambda p: "train" if p <= TRAIN_CUTOFF else "val"
    )
    periods    = stats["academic_period"].tolist()
    x_labels   = [p.replace("_", "-") for p in periods]
    x          = np.arange(len(periods))
    cutoff_idx = periods.index(TRAIN_CUTOFF) if TRAIN_CUTOFF in periods else None

    fig, axes = plt.subplots(2, 1, figsize=(13, 7), sharex=True,
                             gridspec_kw={"hspace": 0.08})

    # — Panel superior: legajos únicos —
    ax1 = axes[0]
    for split, color, label in [("train", C_BLUE, "Entrenamiento (≤ 2022-2C)"),
                                 ("val",   C_ORANGE, "Validación (> 2022-2C)")]:
        mask = stats["split"] == split
        ax1.bar(x[mask.values], stats.loc[mask, "n_legajos"],
                color=color, alpha=0.85, width=0.85, label=label, zorder=3)
    if cutoff_idx is not None:
        ax1.axvline(cutoff_idx + 0.5, color=C_RED, lw=1.4, ls="--", alpha=0.8, zorder=4)
        ax1.text(cutoff_idx + 0.7, stats["n_legajos"].max() * 0.97,
                 "Corte\nentrenamiento", fontsize=8, color=C_RED, va="top")
    ax1.set_ylabel("Legajos únicos en el panel")
    ax1.set_title("Composición del panel longitudinal por período académico")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax1.legend(loc="lower left")
    despine(ax1)

    # — Panel inferior: tasa de dropout_next —
    ax2 = axes[1]
    dropout_pct = stats["dropout_rate"] * 100
    for split, color in [("train", C_BLUE), ("val", C_ORANGE)]:
        mask = stats["split"] == split
        ax2.bar(x[mask.values], dropout_pct[mask.values],
                color=color, alpha=0.85, width=0.85, zorder=3)
    if cutoff_idx is not None:
        ax2.axvline(cutoff_idx + 0.5, color=C_RED, lw=1.4, ls="--", alpha=0.8, zorder=4)
    ax2.set_ylabel("Tasa de abandono siguiente período (%)")
    ax2.set_xlabel("Período académico")
    ax2.set_ylim(0, 100)
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax2.set_xticks(x)
    ax2.set_xticklabels(x_labels, rotation=55, ha="right", fontsize=8)
    despine(ax2)

    fig.tight_layout()
    path = os.path.join(out_dir, "active_students_per_period.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 3. Composición del panel (reemplaza el heatmap) ───────────────────────────
def plot_panel_composition(student_panel, out_dir):
    """
    Stacked bar chart: para cada período muestra cuántas filas del panel
    tienen dropout_next=0 (activo) y dropout_next=1 (abandono).
    Reemplaza el heatmap que mostraba valores constantes.
    """
    comp = (
        student_panel.groupby(["academic_period", "dropout_next"])
        .size()
        .unstack(fill_value=0)
        .rename(columns={0: "Activo (0)", 1: "Abandono (1)"})
        .sort_index()
    )
    comp["split"] = [
        "train" if p <= TRAIN_CUTOFF else "val" for p in comp.index
    ]
    periods  = comp.index.tolist()
    x        = np.arange(len(periods))
    x_labels = [p.replace("_", "-") for p in periods]
    cutoff_idx = periods.index(TRAIN_CUTOFF) if TRAIN_CUTOFF in periods else None

    # Calcular total por período para mostrar en eje secundario (porcentaje abandono)
    comp["total"] = comp["Activo (0)"] + comp["Abandono (1)"]
    comp["pct_abandono"] = comp["Abandono (1)"] / comp["total"] * 100

    fig, ax = plt.subplots(figsize=(13, 5))

    bottom_activo  = comp["Activo (0)"].values
    bottom_abandono = comp["Abandono (1)"].values

    # Barras apiladas — distinguir train vs val con borde
    for i, (split, p) in enumerate(zip(comp["split"], periods)):
        edgecol = "white"
        ax.bar(i, comp.loc[p, "Activo (0)"],  color=C_BLUE,   alpha=0.8 if split == "train" else 0.5,
               edgecolor=edgecol, width=0.85, zorder=3)
        ax.bar(i, comp.loc[p, "Abandono (1)"], bottom=comp.loc[p, "Activo (0)"],
               color=C_RED,   alpha=0.8 if split == "train" else 0.5,
               edgecolor=edgecol, width=0.85, zorder=3)

    # Línea de corte
    if cutoff_idx is not None:
        ax.axvline(cutoff_idx + 0.5, color=C_GRAY, lw=1.5, ls="--", alpha=0.9, zorder=5)
        ax.text(cutoff_idx + 0.7, comp["total"].max() * 0.97,
                "Corte\nentrenamiento", fontsize=8, color=C_GRAY, va="top")

    # Eje secundario: % abandono
    ax2 = ax.twinx()
    ax2.plot(x, comp["pct_abandono"], color=C_ORANGE, lw=2, marker="o",
             markersize=4, zorder=6, label="% abandono en período")
    ax2.set_ylabel("Tasa de abandono (%)", color=C_ORANGE)
    ax2.tick_params(axis="y", colors=C_ORANGE)
    ax2.set_ylim(0, 100)
    ax2.spines["top"].set_visible(False)

    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=55, ha="right", fontsize=8)
    ax.set_ylabel("Filas del panel")
    ax.set_title("Composición del panel longitudinal: balance de clases por período")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    despine(ax)

    # Leyenda manual
    patch_activo  = mpatches.Patch(color=C_BLUE,   alpha=0.8, label="Activo siguiente período (0)")
    patch_abandono = mpatches.Patch(color=C_RED,   alpha=0.8, label="Abandona siguiente período (1)")
    patch_train   = mpatches.Patch(color=C_GRAY,   alpha=0.4, label="Entrenamiento (≤ 2022-2C)")
    patch_val     = mpatches.Patch(color=C_GRAY,   alpha=0.25, label="Validación (> 2022-2C)")
    ax.legend(handles=[patch_activo, patch_abandono, patch_train, patch_val],
              loc="upper left", ncol=2, fontsize=8)
    ax2.legend(loc="upper right", fontsize=8)

    fig.tight_layout()
    path = os.path.join(out_dir, "panel_composition.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 4. Distribuciones de features (2×3 grilla) ──────────────────────────────
def plot_feature_distributions(student_panel, out_dir):
    cols_config = [
        ("materias_en_periodo",    "Materias cursadas\npor período"),
        ("promo_en_periodo",       "Materias aprobadas\npor período"),
        ("nota_media_en_periodo",  "Nota media\npor período"),
        ("materias_win3",          "Materias cursadas\n(ventana 4 períodos)"),
        ("promo_win3",             "Materias aprobadas\n(ventana 4 períodos)"),
        ("nota_win3",              "Nota media\n(ventana 4 períodos)"),
    ]
    existing = [(col, label) for col, label in cols_config if col in student_panel.columns]

    fig, axes = plt.subplots(2, 3, figsize=(12, 7))
    axes = axes.flatten()

    for ax, (col, label) in zip(axes, existing):
        data = student_panel[col].dropna()
        data_nz = data[data > 0]
        ax.hist(data_nz, bins=28, color=C_BLUE, alpha=0.82, edgecolor="white", zorder=3)
        med = data_nz.median()
        ax.axvline(med, color=C_ORANGE, lw=1.6, ls="--", label=f"Mediana = {med:.1f}")
        ax.set_title(label, fontsize=10)
        ax.set_xlabel("Valor")
        ax.set_ylabel("Frecuencia" if ax in axes[[0, 3]] else "")
        ax.legend(fontsize=8, framealpha=0.9)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}"))
        ax.text(0.97, 0.94, f"n = {len(data_nz):,}", transform=ax.transAxes,
                va="top", ha="right", fontsize=8, color=C_GRAY)
        despine(ax)

    # Desactivar panel vacío si hay menos de 6
    for ax in axes[len(existing):]:
        ax.set_visible(False)

    fig.suptitle("Distribución de features académicas (períodos con actividad > 0)",
                 fontsize=12, y=1.01)
    fig.tight_layout()
    path = os.path.join(out_dir, "feature_distributions.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 5. Tasa de abandono por carrera ──────────────────────────────────────────
def plot_dropout_by_carrera(student_panel, out_dir, top_n=20):
    latest = (
        student_panel.sort_values("academic_period")
        .groupby(["legajo", "cod_carrera"])
        .last()
        .reset_index()
    )
    stats = (
        latest.groupby("cod_carrera")
        .agg(total=("legajo", "count"), abandonos=("dropout_next", "sum"))
        .reset_index()
    )
    stats["tasa"] = stats["abandonos"] / stats["total"] * 100
    stats = (
        stats[stats["total"] >= 100]
        .sort_values("total", ascending=False)
        .head(top_n)
        .sort_values("tasa", ascending=True)
        .reset_index(drop=True)
    )

    if stats.empty:
        print("  SKIP: no hay suficientes datos por carrera")
        return

    # Color continuo según tasa
    norm   = plt.Normalize(stats["tasa"].min(), stats["tasa"].max())
    cmap   = plt.cm.RdYlGn_r
    colors = [cmap(norm(v)) for v in stats["tasa"]]

    fig, ax = plt.subplots(figsize=(8, max(4.5, len(stats) * 0.42)))

    bars = ax.barh(range(len(stats)), stats["tasa"], color=colors,
                   edgecolor="white", height=0.68, zorder=3)

    ax.set_yticks(range(len(stats)))
    ax.set_yticklabels(
        [f"{r.cod_carrera}  (n = {r.total:,})" for _, r in stats.iterrows()],
        fontsize=9,
    )
    for bar, val in zip(bars, stats["tasa"]):
        ax.text(val + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val:.0f}%", va="center", ha="left", fontsize=9)
    ax.set_xlabel("Tasa de abandono en siguiente período (%)")
    ax.set_title(
        f"Tasa de abandono por carrera — top {top_n} carreras por volumen de estudiantes",
        fontsize=12,
    )
    ax.set_xlim(0, stats["tasa"].max() * 1.12)
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    despine(ax)

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label="Tasa de abandono (%)", shrink=0.6, pad=0.01)

    fig.tight_layout()
    path = os.path.join(out_dir, "dropout_by_carrera.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 6. Correlación de features con la variable objetivo ──────────────────────
def plot_feature_correlation(student_panel, out_dir):
    num_cols = [
        "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
        "materias_win3", "promo_win3", "nota_win3",
    ]
    labels_es = {
        "materias_en_periodo":    "Materias cursadas (período)",
        "promo_en_periodo":       "Materias aprobadas (período)",
        "nota_media_en_periodo":  "Nota media (período)",
        "materias_win3":          "Materias cursadas (ventana 4p)",
        "promo_win3":             "Materias aprobadas (ventana 4p)",
        "nota_win3":              "Nota media (ventana 4p)",
    }
    existing = [c for c in num_cols if c in student_panel.columns]
    if not existing:
        return

    df   = student_panel[existing + ["dropout_next"]].dropna()
    corr = df[existing].corrwith(df["dropout_next"]).sort_values()

    labels = [labels_es.get(c, c) for c in corr.index]
    colors = [C_RED if v > 0 else C_BLUE for v in corr.values]

    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    bars = ax.barh(labels, corr.values, color=colors, alpha=0.85,
                   edgecolor="white", height=0.62, zorder=3)
    ax.axvline(0, color="black", lw=0.8, alpha=0.6, zorder=4)

    for bar, val in zip(bars, corr.values):
        # Place label just inside the bar end to avoid axis clipping
        offset = 0.003 if val >= 0 else 0.003
        ha     = "left" if val >= 0 else "left"
        ax.text(val + offset, bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}", va="center", ha=ha, fontsize=9)

    ax.set_xlim(corr.min() * 1.25, max(0.01, corr.max() * 1.5 + 0.02))
    ax.set_xlabel("Correlación de Pearson con dropout_next")
    ax.set_title(
        "Correlación de las features académicas con la variable objetivo\n"
        "Valores negativos: mayor actividad asociada a menor riesgo de abandono",
        fontsize=11,
    )
    despine(ax)
    fig.tight_layout()
    path = os.path.join(out_dir, "feature_target_correlation.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── 7. Distribución de probabilidades de predicción ─────────────────────────
def plot_prediction_distribution(engine, out_dir):
    try:
        pred_df = pd.read_sql(
            "SELECT dropout_probability, dropout_prediction "
            "FROM predictions.student_dropout_predictions",
            engine,
        )
    except Exception:
        print("  SKIP: tabla de predicciones no disponible")
        return
    if pred_df.empty:
        print("  SKIP: tabla de predicciones vacía")
        return

    proba  = pred_df["dropout_probability"].values
    n_high = (proba >= 0.7).sum()
    n_med  = ((proba >= 0.5) & (proba < 0.7)).sum()
    n_low  = (proba < 0.5).sum()
    total  = len(proba)

    fig, ax = plt.subplots(figsize=(8, 4.5))

    # Histograma
    ax.hist(proba, bins=40, color=C_BLUE, alpha=0.75, edgecolor="white",
            zorder=3, label="Estudiantes activos")

    # Zonas de riesgo
    ax.axvspan(0.0, 0.5, alpha=0.06, color=C_GREEN, zorder=2)
    ax.axvspan(0.5, 0.7, alpha=0.06, color=C_ORANGE, zorder=2)
    ax.axvspan(0.7, 1.0, alpha=0.10, color=C_RED, zorder=2)

    # Líneas de umbral
    ax.axvline(0.5, color=C_ORANGE, lw=1.5, ls="--", zorder=4)
    ax.axvline(0.7, color=C_RED,    lw=1.5, ls="--", zorder=4)

    ymax = ax.get_ylim()[1]
    for x_pos, label, n, color in [
        (0.25, "Riesgo bajo",  n_low,  C_GREEN),
        (0.60, "Riesgo medio", n_med,  C_ORANGE),
        (0.85, "Riesgo alto",  n_high, C_RED),
    ]:
        ax.text(x_pos, ymax * 0.88,
                f"{label}\n{n:,} est.\n({n/total*100:.1f}%)",
                ha="center", va="top", fontsize=9, color=color,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                          edgecolor=color, alpha=0.85))

    ax.set_xlabel("Probabilidad de abandono predicha")
    ax.set_ylabel("Número de estudiantes")
    ax.set_title(
        f"Distribución del riesgo de abandono — cohorte activa ({total:,} estudiantes)",
        fontsize=12,
    )
    ax.set_xlim(0, 1)
    despine(ax)
    fig.tight_layout()
    path = os.path.join(out_dir, "prediction_distribution.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Guardado: {path}")


# ── Reporte de texto (sin imágenes) ──────────────────────────────────────────
def print_text_summary(student_status, student_panel, engine):
    SEP = "=" * 70
    HR  = "-" * 70

    print(f"\n{SEP}")
    print("  RESUMEN ESTADÍSTICO DEL DATASET — UNDAV")
    print(SEP)

    # 1. Distribución de estados
    print("\n  1. Distribución de estados estudiantiles:")
    print(HR)
    order  = ["estudiando", "graduado", "abandonó"]
    counts = student_status["status"].value_counts().reindex(
        [s for s in order if s in student_status["status"].unique()]
    )
    total = counts.sum()
    print(f"  Total legajos: {total:,}")
    for status, n in counts.items():
        pct  = n / total * 100
        barra = "█" * int(pct / 2)
        print(f"  {STATUS_LABELS.get(status, status):<35}  {n:>8,}  ({pct:5.1f}%)  {barra}")

    # 2. Panel por período
    print(f"\n  2. Panel longitudinal por período (filas y tasa de dropout):")
    print(HR)
    stats = (
        student_panel.groupby("academic_period")
        .agg(n_legajos=("legajo", "nunique"), n_filas=("legajo", "count"),
             dropout_rate=("dropout_next", "mean"))
        .reset_index()
        .sort_values("academic_period")
    )
    print(f"  {'Período':<12}  {'Legajos':>8}  {'Filas':>8}  {'Dropout%':>9}  Split")
    print(f"  {'-'*12}  {'-'*8}  {'-'*8}  {'-'*9}  -----")
    for _, row in stats.iterrows():
        split = "TRAIN" if row["academic_period"] <= TRAIN_CUTOFF else "val  "
        print(f"  {row['academic_period']:<12}  {int(row['n_legajos']):>8,}  "
              f"{int(row['n_filas']):>8,}  {row['dropout_rate']*100:>8.1f}%  {split}")

    # 3. Estadísticas de features
    print(f"\n  3. Estadísticas de features numéricas (períodos con actividad > 0):")
    print(HR)
    num_cols = [
        "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
        "materias_win3", "promo_win3", "nota_win3",
    ]
    labels = {
        "materias_en_periodo":   "Materias cursadas (período)",
        "promo_en_periodo":      "Materias aprobadas (período)",
        "nota_media_en_periodo": "Nota media (período)",
        "materias_win3":         "Materias cursadas (ventana 4p)",
        "promo_win3":            "Materias aprobadas (ventana 4p)",
        "nota_win3":             "Nota media (ventana 4p)",
    }
    print(f"  {'Feature':<30}  {'n>0':>7}  {'Media':>7}  {'Mediana':>8}  {'Std':>7}  {'p90':>7}")
    print(f"  {'-'*30}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*7}  {'-'*7}")
    for col in num_cols:
        if col not in student_panel.columns:
            continue
        s    = pd.to_numeric(student_panel[col], errors="coerce")
        s_nz = s[s > 0]
        if s_nz.empty:
            continue
        print(f"  {labels.get(col, col):<30}  {len(s_nz):>7,}  {s_nz.mean():>7.2f}  "
              f"{s_nz.median():>8.2f}  {s_nz.std():>7.2f}  {s_nz.quantile(0.9):>7.2f}")

    # 4. Balance de clases train/val
    print(f"\n  4. Balance de clases — train vs validación:")
    print(HR)
    for split_label, cond in [("TRAIN", student_panel["academic_period"] <= TRAIN_CUTOFF),
                               ("VAL  ", student_panel["academic_period"] > TRAIN_CUTOFF)]:
        sub = student_panel[cond]
        n   = len(sub)
        if n == 0:
            print(f"  {split_label}: (sin datos)")
            continue
        n1  = int(sub["dropout_next"].sum())
        n0  = n - n1
        print(f"  {split_label}  total={n:,}  abandono(1)={n1:,} ({n1/n*100:.1f}%)  "
              f"activo(0)={n0:,} ({n0/n*100:.1f}%)")

    # 5. Tasa de abandono por carrera
    print(f"\n  5. Tasa de abandono por carrera (top 15 por volumen):")
    print(HR)
    latest = (
        student_panel.sort_values("academic_period")
        .groupby(["legajo", "cod_carrera"])
        .last()
        .reset_index()
    )
    by_carrera = (
        latest.groupby("cod_carrera")
        .agg(total=("legajo", "count"), abandonos=("dropout_next", "sum"))
        .reset_index()
    )
    by_carrera["tasa"] = by_carrera["abandonos"] / by_carrera["total"] * 100
    by_carrera = (
        by_carrera[by_carrera["total"] >= 50]
        .sort_values("total", ascending=False)
        .head(15)
        .sort_values("tasa", ascending=False)
    )
    print(f"  {'Carrera':<20}  {'Total':>7}  {'Abandonos':>10}  {'Tasa%':>7}")
    print(f"  {'-'*20}  {'-'*7}  {'-'*10}  {'-'*7}")
    for _, row in by_carrera.iterrows():
        print(f"  {str(row['cod_carrera']):<20}  {int(row['total']):>7,}  "
              f"{int(row['abandonos']):>10,}  {row['tasa']:>6.1f}%")

    # 6. Correlaciones con dropout_next
    print(f"\n  6. Correlaciones de Pearson con dropout_next:")
    print(HR)
    cols_corr = [
        "materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
        "materias_win3", "promo_win3", "nota_win3",
    ]
    df_corr = student_panel[cols_corr + ["dropout_next"]].dropna()
    corr    = df_corr[cols_corr].corrwith(df_corr["dropout_next"]).sort_values()
    for col, val in corr.items():
        direction = "↓ riesgo" if val < 0 else "↑ riesgo"
        barra     = "█" * int(abs(val) * 40)
        print(f"  {labels.get(col, col):<35}  {val:>7.4f}  {direction}  {barra}")

    # 7. Predicciones si están disponibles
    try:
        pred_df = pd.read_sql(
            "SELECT dropout_probability, dropout_prediction FROM predictions.student_dropout_predictions",
            engine,
        )
        if not pred_df.empty:
            proba  = pred_df["dropout_probability"]
            n_tot  = len(pred_df)
            n_high = (proba >= 0.7).sum()
            n_med  = ((proba >= 0.5) & (proba < 0.7)).sum()
            n_low  = (proba < 0.5).sum()
            print(f"\n  7. Distribución de riesgo en cohorte activa ({n_tot:,} estudiantes):")
            print(HR)
            print(f"  Riesgo alto  (>= 0.70): {n_high:>5,}  ({n_high/n_tot*100:.1f}%)")
            print(f"  Riesgo medio [0.5-0.7): {n_med:>5,}  ({n_med/n_tot*100:.1f}%)")
            print(f"  Riesgo bajo  (<  0.50): {n_low:>5,}  ({n_low/n_tot*100:.1f}%)")
            print(f"  Probabilidad media     : {proba.mean():.4f}")
            print(f"  Probabilidad mediana   : {proba.median():.4f}")
    except Exception:
        pass

    print(f"\n{SEP}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print("  Generando plots del dataset — UNDAV")
    print(f"{'='*60}\n")

    engine = create_engine(PG_URI)

    try:
        n = pd.read_sql("SELECT COUNT(*) AS n FROM marts.student_panel", engine).iloc[0]["n"]
        if n == 0:
            print("ERROR: marts.student_panel vacía. Corré refresh_canonical primero.")
            return
        print(f"  student_panel: {n:,} filas\n")
    except Exception as e:
        print(f"ERROR: {e}"); return

    print("Cargando datos...")
    student_status = pd.read_sql("SELECT * FROM marts.student_status", engine)
    student_panel  = pd.read_sql("SELECT * FROM marts.student_panel", engine)
    print(f"  student_status : {len(student_status):,}")
    print(f"  student_panel  : {len(student_panel):,}\n")

    # Reporte de texto primero
    print_text_summary(student_status, student_panel, engine)

    print("Generando figuras...")
    plot_student_status(student_status, THESIS_FIGS_DIR)
    plot_active_per_period(student_panel, THESIS_FIGS_DIR)
    plot_panel_composition(student_panel, THESIS_FIGS_DIR)
    plot_feature_distributions(student_panel, THESIS_FIGS_DIR)
    plot_dropout_by_carrera(student_panel, THESIS_FIGS_DIR)
    plot_feature_correlation(student_panel, THESIS_FIGS_DIR)
    plot_prediction_distribution(engine, THESIS_FIGS_DIR)

    print(f"\n  Listo. Figuras en {THESIS_FIGS_DIR}\n")


if __name__ == "__main__":
    main()
