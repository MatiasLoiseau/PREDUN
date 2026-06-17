"""
Evaluación de EQUIDAD por subgrupo del modelo de abandono.

Responde a la observación del jurado (Q11, prioridad 2.3): excluir las variables
demográficas del entrenamiento no garantiza ausencia de sesgo, porque las features
académicas pueden actuar como proxies. Se evalúa el desempeño del modelo por
subpoblación, no solo la tasa de abandono observada.

Protocolo: se reproduce el modelo de referencia del backtest (GradientBoosting con
EMBARGO de maduración, corte de entrenamiento 2021_1C, prueba 2023_1C) y se evalúa
sobre el período de prueba, segmentando por atributos demográficos de
canonical.alumnos NO usados como features: sexo, cohorte de ingreso, edad al
ingreso y carrera. Para cada subgrupo se reporta n, prevalencia, AUC, TPR (recall),
FPR, precisión, calibración (pendiente, ECE) y Precision@10%.

Uso:
    conda run -n eda-predun python scripts/fairness_subgroups.py
"""
import os
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, brier_score_loss

from backtest_temporal import (
    PG_URI, build_pipeline, calibration, ece, FEATURES_NUM, FEATURES_CAT,
    shift_period, LABEL_HORIZON,
)

warnings.filterwarnings("ignore")

THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter5"
)
TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C (embargo)
MIN_GROUP = 150   # subgrupos con menos casos no se reportan (ruido)


def group_metrics(y, p, k_frac=0.10):
    y = np.asarray(y); p = np.asarray(p)
    pred = (p >= 0.5).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum()); fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum()); tn = int(((pred == 0) & (y == 0)).sum())
    tpr = tp / (tp + fn) if (tp + fn) else float("nan")
    fpr = fp / (fp + tn) if (fp + tn) else float("nan")
    prec = tp / (tp + fp) if (tp + fp) else float("nan")
    auc = roc_auc_score(y, p) if len(np.unique(y)) > 1 else float("nan")
    try:
        slope, _ = calibration(y, p)
    except Exception:
        slope = float("nan")
    order = np.argsort(-p)
    topk = order[:max(int(len(p) * k_frac), 1)]
    p_at_k = float(y[topk].mean())
    return dict(
        n=len(y), prevalencia=round(float(y.mean()), 4), auc=round(auc, 4),
        tpr=round(tpr, 4), fpr=round(fpr, 4), precision=round(prec, 4),
        calib_slope=round(slope, 3), ece=round(ece(y, p), 4),
        brier=round(brier_score_loss(y, p), 4), p_at_10=round(p_at_k, 4),
    )


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1 AND dropout_next IS NOT NULL",
        engine,
    )
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)

    # Demografía: una fila por legajo (atributos estables / primer registro)
    alu = pd.read_sql(text("""
        SELECT legajo, sexo, fecha_nacimiento, fecha_inscripcion
        FROM canonical.alumnos
    """), engine)
    alu["legajo"] = alu["legajo"].astype(str)
    for c in ("fecha_nacimiento", "fecha_inscripcion"):
        alu[c] = pd.to_datetime(alu[c], errors="coerce")
    demo = (alu.sort_values("fecha_inscripcion")
               .groupby("legajo")
               .agg(sexo=("sexo", "first"),
                    anio_nac=("fecha_nacimiento", lambda s: s.dropna().dt.year.min() if s.notna().any() else np.nan),
                    anio_ing=("fecha_inscripcion", lambda s: s.dropna().dt.year.min() if s.notna().any() else np.nan))
               .reset_index())
    demo["edad_ingreso"] = demo["anio_ing"] - demo["anio_nac"]
    demo["sexo"] = demo["sexo"].astype(str).str.strip().str.upper().replace({"NAN": None})
    demo["cohorte"] = pd.cut(demo["anio_ing"], [0, 2014, 2017, 2020, 2100],
                             labels=["<=2014", "2015-2017", "2018-2020", ">=2021"])
    demo["grupo_edad"] = pd.cut(demo["edad_ingreso"], [0, 20, 25, 30, 120],
                                labels=["<=20", "21-25", "26-30", ">30"])

    df["legajo"] = df["legajo"].astype(str)

    tr = df[df.academic_period <= TRAIN_CUTOFF]
    te = df[df.academic_period == TEST_PERIOD].merge(demo, on="legajo", how="left")
    print(f"Entrenamiento <= {TRAIN_CUTOFF}: {len(tr):,} filas | Prueba {TEST_PERIOD}: {len(te):,} filas")

    model = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr[FEATURES_NUM + FEATURES_CAT], tr["dropout_next"].values)
    te = te.copy()
    te["p"] = model.predict_proba(te[FEATURES_NUM + FEATURES_CAT])[:, 1]

    overall = group_metrics(te["dropout_next"].values, te["p"].values)
    print("\n=== Desempeño global (test 2023_1C) ===")
    print(pd.DataFrame([{"grupo": "GLOBAL", "valor": "—", **overall}]).to_string(index=False))

    rows = []
    for attr, label in [("sexo", "Sexo"), ("cohorte", "Cohorte de ingreso"),
                        ("grupo_edad", "Edad al ingreso"), ("cod_carrera", "Carrera")]:
        sub = te.dropna(subset=[attr])
        for val, g in sub.groupby(attr, observed=True):
            if len(g) < MIN_GROUP or g["dropout_next"].nunique() < 2:
                continue
            m = group_metrics(g["dropout_next"].values, g["p"].values)
            rows.append({"atributo": label, "valor": str(val), **m})

    res = pd.DataFrame(rows)
    pd.set_option("display.width", 220, "display.max_columns", 40)
    for label in ["Sexo", "Cohorte de ingreso", "Edad al ingreso"]:
        blk = res[res.atributo == label]
        if not blk.empty:
            print(f"\n=== {label} ===")
            print(blk.drop(columns=["atributo"]).to_string(index=False))
    carr = res[res.atributo == "Carrera"].sort_values("auc")
    if not carr.empty:
        print(f"\n=== Carrera (extremos de AUC; n>= {MIN_GROUP}) ===")
        print(pd.concat([carr.head(5), carr.tail(5)]).drop(columns=["atributo"]).to_string(index=False))
        print(f"\nDispersión de AUC entre carreras: min={carr.auc.min():.3f} "
              f"max={carr.auc.max():.3f} (rango={carr.auc.max()-carr.auc.min():.3f})")

    # Figura: AUC, TPR y FPR por sexo y cohorte
    _set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)
    plot_df = res[res.atributo.isin(["Sexo", "Cohorte de ingreso"])].copy()
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    for ax, label in zip(axes, ["Sexo", "Cohorte de ingreso"]):
        blk = plot_df[plot_df.atributo == label]
        x = np.arange(len(blk)); w = 0.25
        ax.bar(x - w, blk["auc"], w, label="AUC", color="#1565C0")
        ax.bar(x, blk["tpr"], w, label="TPR", color="#2E7D32")
        ax.bar(x + w, blk["fpr"], w, label="FPR", color="#B71C1C")
        ax.axhline(overall["auc"], color="#1565C0", ls=":", lw=1, alpha=0.7)
        ax.set_xticks(x); ax.set_xticklabels(blk["valor"], rotation=10)
        ax.set_title(label); ax.set_ylim(0, 1); ax.legend(fontsize=8)
    fig.suptitle("Desempeño del modelo por subgrupo demográfico (test 2023-1C)", y=1.02)
    fig.tight_layout()
    path = os.path.join(THESIS_FIGS_DIR, "fairness_subgroups.png")
    fig.savefig(path); plt.close(fig)
    print(f"\nGuardado: {path}")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.fairness_subgroups"))
        conn.commit()
    res.to_sql("fairness_subgroups", engine, schema="predictions", if_exists="replace", index=False)
    print("Resultados persistidos en predictions.fairness_subgroups")


def _set_style():
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        pass
    plt.rcParams.update({
        "font.family": "serif", "font.size": 11, "axes.titlesize": 12,
        "axes.labelsize": 11, "legend.fontsize": 9, "savefig.dpi": 300,
        "savefig.bbox": "tight", "savefig.pad_inches": 0.1,
    })


if __name__ == "__main__":
    main()
