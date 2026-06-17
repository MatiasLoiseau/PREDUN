"""
(F) Importancia por permutación  +  deriva (PSI) por segmento.

Responde a dos observaciones del jurado:

  3c: la importancia por impureza tiene un sesgo conocido y no debería ser la
      única evidencia interpretativa. Se reporta la importancia por PERMUTACIÓN
      sobre el conjunto de validación (caída de AUC al permutar cada feature),
      que es un estimador del efecto sobre el desempeño y no del split.

  2.8: el análisis de deriva global puede ocultar deriva localizada. Se calcula
       el PSI de una feature de promoción (alta deriva global) y de la variable
       objetivo DENTRO de segmentos (sexo, cohorte de ingreso) para mostrar si la
       deriva es uniforme o concentrada en subpoblaciones.

Protocolo: modelo de referencia (corte 2021_1C, prueba 2023_1C). Referencia de
deriva: filas en riesgo <= 2021_1C; actual: filas en riesgo > 2021_1C.

Uso:
    conda run -n eda-predun python scripts/permutation_and_segment_drift.py
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
from sklearn.inspection import permutation_importance

from backtest_temporal import PG_URI, build_pipeline, FEATURES_NUM, FEATURES_CAT, shift_period, LABEL_HORIZON

warnings.filterwarnings("ignore")

THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter5"
)
TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C


def psi_continuous(ref, cur, bins=10, eps=1e-6):
    ref = pd.to_numeric(ref, errors="coerce").dropna()
    cur = pd.to_numeric(cur, errors="coerce").dropna()
    if len(ref) < 50 or len(cur) < 50:
        return float("nan")
    edges = np.unique(np.percentile(ref, np.linspace(0, 100, bins + 1)))
    if len(edges) < 3:
        return float("nan")
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / len(cur)
    r = np.clip(r, eps, None); c = np.clip(c, eps, None)
    return float(np.sum((c - r) * np.log(c / r)))


def psi_binary(ref, cur, eps=1e-6):
    r = np.array([1 - ref.mean(), ref.mean()])
    c = np.array([1 - cur.mean(), cur.mean()])
    r = np.clip(r, eps, None); c = np.clip(c, eps, None)
    return float(np.sum((c - r) * np.log(c / r)))


def _set_style():
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        pass
    plt.rcParams.update({"font.family": "serif", "font.size": 11, "savefig.dpi": 300,
                         "savefig.bbox": "tight"})


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql("SELECT * FROM marts.student_panel WHERE at_risk = 1", engine)
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["legajo"] = df["legajo"].astype(str)

    lab = df[df["dropout_next"].notna()].copy()
    lab["dropout_next"] = lab["dropout_next"].astype(int)
    tr = lab[lab.academic_period <= TRAIN_CUTOFF]
    te = lab[lab.academic_period == TEST_PERIOD]

    model = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr[FEATURES_NUM + FEATURES_CAT], tr["dropout_next"].values)

    # ── (F1) Importancia por permutación sobre el conjunto de validación ─────────
    Xte = te[FEATURES_NUM + FEATURES_CAT]
    yte = te["dropout_next"].values
    pi = permutation_importance(model, Xte, yte, scoring="roc_auc",
                                n_repeats=10, random_state=42, n_jobs=-1)
    imp = pd.DataFrame({
        "feature": FEATURES_NUM + FEATURES_CAT,
        "caida_auc_media": np.round(pi.importances_mean, 4),
        "std": np.round(pi.importances_std, 4),
    }).sort_values("caida_auc_media", ascending=False)
    pd.set_option("display.width", 200, "display.max_columns", 20)
    print("=== (F1) Importancia por permutación (caída de AUC al permutar, test 2023_1C) ===")
    print(imp.to_string(index=False))

    _set_style()
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 5))
    blk = imp.iloc[::-1]
    ax.barh(blk["feature"], blk["caida_auc_media"], xerr=blk["std"],
            color="#1565C0", alpha=0.85, edgecolor="white")
    ax.set_xlabel("Caída de ROC-AUC al permutar la feature")
    ax.set_title("Importancia por permutación en validación (modelo GradientBoosting)")
    fig.tight_layout()
    path = os.path.join(THESIS_FIGS_DIR, "permutation_importance.png")
    fig.savefig(path); plt.close(fig)
    print(f"Guardado: {path}")

    # ── (F2) PSI por segmento ────────────────────────────────────────────────────
    alu = pd.read_sql(text("""
        SELECT legajo, sexo, fecha_inscripcion FROM canonical.alumnos
    """), engine)
    alu["legajo"] = alu["legajo"].astype(str)
    alu["fecha_inscripcion"] = pd.to_datetime(alu["fecha_inscripcion"], errors="coerce")
    demo = (alu.sort_values("fecha_inscripcion").groupby("legajo")
              .agg(sexo=("sexo", "first"),
                   anio_ing=("fecha_inscripcion", lambda s: s.dropna().dt.year.min() if s.notna().any() else np.nan))
              .reset_index())
    demo["sexo"] = demo["sexo"].astype(str).str.strip().str.upper().replace({"NAN": None})
    demo["cohorte"] = pd.cut(demo["anio_ing"], [0, 2017, 2020, 2100],
                             labels=["<=2017", "2018-2020", ">=2021"])

    ref = df[df.academic_period <= TRAIN_CUTOFF].merge(demo, on="legajo", how="left")
    cur = df[df.academic_period > TRAIN_CUTOFF].merge(demo, on="legajo", how="left")

    seg_rows = []
    for attr in ["sexo", "cohorte"]:
        for val in [v for v in ref[attr].dropna().unique()]:
            r = ref[ref[attr] == val]; c = cur[cur[attr] == val]
            if len(r) < 200 or len(c) < 200:
                continue
            rl = r[r["dropout_next"].notna()]; cl = c[c["dropout_next"].notna()]
            seg_rows.append(dict(
                segmento=f"{attr}={val}", n_ref=len(r), n_act=len(c),
                psi_promo_rate_win3=round(psi_continuous(r["promo_rate_win3"], c["promo_rate_win3"]), 3),
                psi_dias=round(psi_continuous(r["dias_desde_ult_actividad"], c["dias_desde_ult_actividad"]), 3),
                psi_dropout_next=round(psi_binary(rl["dropout_next"].astype(int),
                                                  cl["dropout_next"].astype(int)), 3),
            ))
    seg = pd.DataFrame(seg_rows)
    print("\n=== (F2) PSI por segmento (referencia <=2021_1C vs actual >2021_1C) ===")
    print(seg.to_string(index=False))
    print("\nUmbrales PSI (heurísticos, no inferenciales): <0,10 sin deriva; "
          "0,10–0,25 moderada; >=0,25 alta.")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.permutation_importance"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.drift_by_segment"))
        conn.commit()
    imp.to_sql("permutation_importance", engine, schema="predictions", if_exists="replace", index=False)
    seg.to_sql("drift_by_segment", engine, schema="predictions", if_exists="replace", index=False)
    print("\nPersistido en predictions.permutation_importance y predictions.drift_by_segment")


if __name__ == "__main__":
    main()
