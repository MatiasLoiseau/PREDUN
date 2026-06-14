"""
Backtest temporal de origen móvil para PREDUN (evaluación de generalización).

Para cada origen, entrena sobre las filas en riesgo con etiqueta observable
hasta ese período (academic_period <= origen) y evalúa sobre el período
siguiente plenamente etiquetable. Reporta, por período de prueba:

  - AUC con IC 95% por bootstrap AGRUPADO por legajo (respeta la correlación
    entre observaciones de un mismo estudiante).
  - Baseline operativo: ranking por dias_desde_ult_actividad (regla de recencia).
  - Brier, Brier Skill Score (vs predictor constante en la prevalencia), KS.
  - Calibración: pendiente e intercepto de recalibración logística, ECE.
  - Precision@K / Recall@K sobre estudiantes únicos del período.

Las features derivadas (materias_cum, promo_rate_*) se leen de marts.student_panel
(calculadas en dbt), garantizando consistencia con entrenamiento y scoring.

Uso:
    conda run -n eda-predun python scripts/backtest_temporal.py
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
from sklearn.metrics import (
    roc_auc_score, brier_score_loss, f1_score, precision_score, recall_score,
)

warnings.filterwarnings("ignore")

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")

NUM = ["materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
       "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_actividad"]
DER = ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURES_NUM = NUM + DER
FEATURES_CAT = ["cod_carrera"]

# Origen de entrenamiento -> primer período plenamente etiquetable posterior.
ORIGINS = [("2021_2C", "2022_1C"), ("2022_1C", "2022_2C"), ("2022_2C", "2023_1C")]


def build_pipeline(clf):
    pre = ColumnTransformer([
        ("num", Pipeline([("i", SimpleImputer(strategy="median")),
                          ("s", StandardScaler())]), FEATURES_NUM),
        ("cat", Pipeline([("i", SimpleImputer(strategy="most_frequent")),
                          ("e", OneHotEncoder(handle_unknown="ignore"))]), FEATURES_CAT),
    ])
    return Pipeline([("prep", pre), ("model", clf)])


def ks_stat(y, p):
    pp = np.sort(p[y == 1]); pn = np.sort(p[y == 0]); c = np.sort(np.concatenate([pp, pn]))
    cp = np.searchsorted(pp, c, "right") / max(len(pp), 1)
    cn = np.searchsorted(pn, c, "right") / max(len(pn), 1)
    return float(np.max(np.abs(cp - cn)))


def calibration(y, p):
    eps = 1e-6
    logit = np.log(np.clip(p, eps, 1 - eps) / (1 - np.clip(p, eps, 1 - eps))).reshape(-1, 1)
    m = LogisticRegression(solver="lbfgs").fit(logit, y)
    return float(m.coef_[0, 0]), float(m.intercept_[0])


def ece(y, p, bins=10):
    e = 0.0; edges = np.linspace(0, 1, bins + 1)
    for i in range(bins):
        hi = edges[i + 1]
        m = (p >= edges[i]) & (p < hi) if i < bins - 1 else (p >= edges[i]) & (p <= hi)
        if m.sum() > 0:
            e += m.sum() / len(p) * abs(y[m].mean() - p[m].mean())
    return float(e)


def grouped_bootstrap_auc(y, p, groups, n=300, seed=42):
    rng = np.random.default_rng(seed)
    uniq = np.unique(groups)
    idx = {g: np.where(groups == g)[0] for g in uniq}
    aucs = []
    for _ in range(n):
        samp = rng.choice(uniq, len(uniq), replace=True)
        ii = np.concatenate([idx[g] for g in samp])
        ys = y[ii]
        if ys.min() == ys.max():
            continue
        aucs.append(roc_auc_score(ys, p[ii]))
    return float(np.percentile(aucs, 2.5)), float(np.percentile(aucs, 97.5))


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1 AND dropout_next IS NOT NULL",
        engine,
    )
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)

    rows = []
    last_model = None
    for origin, test in ORIGINS:
        tr = df[df.academic_period <= origin]
        te = df[df.academic_period == test]
        Xtr, ytr = tr[FEATURES_NUM + FEATURES_CAT], tr["dropout_next"].values
        Xte, yte = te[FEATURES_NUM + FEATURES_CAT], te["dropout_next"].values
        leg = te["legajo"].values

        model = build_pipeline(GradientBoostingClassifier(
            n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(Xtr, ytr)
        last_model = model
        p = model.predict_proba(Xte)[:, 1]
        pred = (p >= 0.5).astype(int)

        prev = float(yte.mean())
        brier = brier_score_loss(yte, p)
        bss = 1 - brier / (prev * (1 - prev))
        slope, intercept = calibration(yte, p)
        lo, hi = grouped_bootstrap_auc(yte, p, leg)

        base = te["dias_desde_ult_actividad"].fillna(
            te["dias_desde_ult_actividad"].median()).values
        base_auc = roc_auc_score(yte, base)

        order = np.argsort(-p)
        pk = {k: float(yte[order[:int(len(p) * k / 100)]].mean()) for k in (5, 10, 20, 30)}
        rk = {k: float(yte[order[:int(len(p) * k / 100)]].sum() / yte.sum()) for k in (5, 10, 20, 30)}

        rows.append(dict(
            origin=origin, test_period=test, n_train=len(tr), n_test=len(te), prevalence=round(prev, 4),
            auc=round(roc_auc_score(yte, p), 4), auc_ci_low=round(lo, 4), auc_ci_high=round(hi, 4),
            baseline_recency_auc=round(base_auc, 4),
            brier=round(brier, 4), brier_skill_score=round(bss, 4), ks=round(ks_stat(yte, p), 4),
            f1=round(f1_score(yte, pred), 4), precision=round(precision_score(yte, pred), 4),
            recall=round(recall_score(yte, pred), 4),
            calib_slope=round(slope, 3), calib_intercept=round(intercept, 3), ece=round(ece(yte, p), 4),
            p_at_5=round(pk[5], 4), p_at_10=round(pk[10], 4), p_at_20=round(pk[20], 4), p_at_30=round(pk[30], 4),
            r_at_5=round(rk[5], 4), r_at_10=round(rk[10], 4), r_at_20=round(rk[20], 4), r_at_30=round(rk[30], 4),
        ))

    res = pd.DataFrame(rows)
    pd.set_option("display.width", 220, "display.max_columns", 60)
    print(res.to_string(index=False))

    # Importancia de features del último modelo (origen 2022_2C)
    imp = last_model.named_steps["model"].feature_importances_
    num_imp = list(zip(FEATURES_NUM, imp[:len(FEATURES_NUM)]))
    cat_imp = float(imp[len(FEATURES_NUM):].sum())
    print("\nImportancia de features (modelo origen 2022_2C):")
    for n, v in sorted(num_imp, key=lambda x: -x[1]):
        print(f"  {n:28s} {v:.3f}")
    print(f"  {'cod_carrera (one-hot)':28s} {cat_imp:.3f}")

    # Persistir resultados para trazabilidad / Superset
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.backtest_results"))
        conn.commit()
    res.to_sql("backtest_results", engine, schema="predictions", if_exists="replace", index=False)
    print("\nResultados persistidos en predictions.backtest_results")


if __name__ == "__main__":
    main()
