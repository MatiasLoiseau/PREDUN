"""
(D) Ablación de las features de recencia  +  (E) prueba pareada de ΔAUC.

Responde a dos observaciones del jurado:

  D (Q6 / 2.4): ¿cuánta señal aporta el modelo POR ENCIMA de detectar que el
     estudiante ya dejó de cursar? Se compara el modelo completo contra el mismo
     modelo (i) sin 'dias_desde_ult_actividad' y (ii) sin las features de recencia
     inmediata (recencia + actividad del período corriente), dejando solo la
     trayectoria de ventana/acumulada.

  E (5a): los intervalos de confianza que no se solapan NO sustituyen una prueba
     de diferencia pareada. Se reporta ΔAUC con IC 95% por bootstrap PAREADO y
     AGRUPADO por legajo (modelo vs baseline de recencia, y GBM vs LogisticReg),
     evaluado sobre el MISMO conjunto de prueba.

Protocolo común: corte de entrenamiento 2021_1C, prueba 2023_1C (embargo de 4
períodos), idéntico al modelo de referencia de la tesis.

Uso:
    conda run -n eda-predun python scripts/ablation_and_paired.py
"""
import os
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from backtest_temporal import PG_URI, FEATURES_NUM, FEATURES_CAT, shift_period, LABEL_HORIZON

warnings.filterwarnings("ignore")

TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C

RECENCY_IMMEDIATE = ["dias_desde_ult_actividad", "materias_en_periodo",
                     "promo_en_periodo", "nota_media_en_periodo"]


def build(num, cat, clf):
    pre = ColumnTransformer([
        ("num", Pipeline([("i", SimpleImputer(strategy="median")),
                          ("s", StandardScaler())]), num),
        ("cat", Pipeline([("i", SimpleImputer(strategy="most_frequent")),
                          ("e", OneHotEncoder(handle_unknown="ignore"))]), cat),
    ])
    return Pipeline([("prep", pre), ("model", clf)])


def gbm():
    return GradientBoostingClassifier(n_estimators=100, max_depth=3, subsample=0.8, random_state=42)


def p_at_k(y, p, k=0.10):
    order = np.argsort(-p)
    return float(np.asarray(y)[order[:max(int(len(p) * k), 1)]].mean())


def paired_delta_auc(y, p1, p2, groups, n=500, seed=42):
    """ΔAUC = AUC(p1) - AUC(p2), bootstrap pareado agrupado por legajo."""
    rng = np.random.default_rng(seed)
    uniq = np.unique(groups)
    idx = {g: np.where(groups == g)[0] for g in uniq}
    d = []
    for _ in range(n):
        samp = rng.choice(uniq, len(uniq), replace=True)
        ii = np.concatenate([idx[g] for g in samp])
        ys = y[ii]
        if ys.min() == ys.max():
            continue
        d.append(roc_auc_score(ys, p1[ii]) - roc_auc_score(ys, p2[ii]))
    d = np.array(d)
    return dict(delta=round(float(d.mean()), 4),
                ci_low=round(float(np.percentile(d, 2.5)), 4),
                ci_high=round(float(np.percentile(d, 97.5)), 4),
                p_gt_0=round(float((d > 0).mean()), 4))


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1 AND dropout_next IS NOT NULL",
        engine,
    )
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)

    tr = df[df.academic_period <= TRAIN_CUTOFF]
    te = df[df.academic_period == TEST_PERIOD]
    y = te["dropout_next"].values
    leg = te["legajo"].astype(str).values
    print(f"Entrenamiento <= {TRAIN_CUTOFF}: {len(tr):,} | Prueba {TEST_PERIOD}: {len(te):,} "
          f"(prevalencia {y.mean():.3f})\n")

    # ── (D) Ablación ───────────────────────────────────────────────────────────
    variants = {
        "completo": FEATURES_NUM,
        "sin dias_desde_ult_actividad": [c for c in FEATURES_NUM if c != "dias_desde_ult_actividad"],
        "sin recencia inmediata": [c for c in FEATURES_NUM if c not in RECENCY_IMMEDIATE],
    }
    preds = {}
    abl_rows = []
    for name, num in variants.items():
        m = build(num, FEATURES_CAT, gbm()).fit(tr[num + FEATURES_CAT], tr["dropout_next"].values)
        p = m.predict_proba(te[num + FEATURES_CAT])[:, 1]
        preds[name] = p
        abl_rows.append(dict(variante=name, n_features=len(num) + 1,
                             auc=round(roc_auc_score(y, p), 4), p_at_10=round(p_at_k(y, p), 4)))

    # baseline de recencia puro (ordena por dias_desde_ult_actividad)
    base = te["dias_desde_ult_actividad"].fillna(te["dias_desde_ult_actividad"].median()).values.astype(float)
    preds["baseline recencia"] = base
    abl_rows.append(dict(variante="baseline recencia (solo dias)", n_features=1,
                         auc=round(roc_auc_score(y, base), 4), p_at_10=round(p_at_k(y, base), 4)))

    # LogisticRegression completa (para la comparación pareada GBM vs LR)
    m_lr = build(FEATURES_NUM, FEATURES_CAT, LogisticRegression(
        max_iter=300, solver="saga", random_state=42)).fit(
        tr[FEATURES_NUM + FEATURES_CAT], tr["dropout_next"].values)
    preds["logreg"] = m_lr.predict_proba(te[FEATURES_NUM + FEATURES_CAT])[:, 1]

    abl = pd.DataFrame(abl_rows)
    pd.set_option("display.width", 200, "display.max_columns", 30)
    print("=== (D) Ablación de features de recencia (test 2023_1C) ===")
    print(abl.to_string(index=False))
    full_auc = abl.loc[abl.variante == "completo", "auc"].iloc[0]
    noimm_auc = abl.loc[abl.variante == "sin recencia inmediata", "auc"].iloc[0]
    print(f"\nEl modelo SIN recencia inmediata (solo trayectoria de ventana/acumulada) "
          f"mantiene AUC={noimm_auc:.3f}\nfrente a {full_auc:.3f} del completo: la "
          f"trayectoria aporta señal predictiva por sí sola.")

    # ── (E) ΔAUC pareado ────────────────────────────────────────────────────────
    comparisons = [
        ("completo  −  baseline recencia", preds["completo"], preds["baseline recencia"]),
        ("completo  −  sin recencia inmediata", preds["completo"], preds["sin recencia inmediata"]),
        ("GBM completo  −  LogisticRegression", preds["completo"], preds["logreg"]),
    ]
    pair_rows = []
    for name, p1, p2 in comparisons:
        r = paired_delta_auc(y, np.asarray(p1, float), np.asarray(p2, float), leg)
        pair_rows.append(dict(comparacion=name, **r))
    pair = pd.DataFrame(pair_rows)
    print("\n=== (E) ΔAUC pareado, bootstrap agrupado por legajo (IC 95%, n=500) ===")
    print(pair.to_string(index=False))
    print("\n'p_gt_0' = fracción de remuestreos con ΔAUC>0 (probabilidad de que el "
          "primero supere al segundo).")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.ablation_recency"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.delta_auc_paired"))
        conn.commit()
    abl.to_sql("ablation_recency", engine, schema="predictions", if_exists="replace", index=False)
    pair.to_sql("delta_auc_paired", engine, schema="predictions", if_exists="replace", index=False)
    print("\nPersistido en predictions.ablation_recency y predictions.delta_auc_paired")


if __name__ == "__main__":
    main()
