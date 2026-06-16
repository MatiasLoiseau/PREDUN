"""
Backtest temporal de origen móvil para PREDUN (evaluación de generalización).

Para cada período de prueba P, entrena sobre las filas en riesgo con etiqueta
observable hasta P - LABEL_HORIZON (EMBARGO de maduración: ninguna etiqueta de
entrenamiento depende de actividad de P o posterior) y evalúa sobre P. Reporta,
por período de prueba:

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
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score, roc_curve, brier_score_loss, f1_score, precision_score, recall_score,
)

warnings.filterwarnings("ignore")

PG_URI = os.getenv("PG_URI", "postgresql://siu:siu@localhost:5432/postgres")
THESIS_FIGS_DIR = (
    "/Users/matiasloiseau/Library/CloudStorage/Dropbox/ITBA/tesis/informe/figs/chapter5"
)
PERIOD_COLORS = ["#1565C0", "#2E7D32", "#E65100", "#6A1B9A"]

NUM = ["materias_en_periodo", "promo_en_periodo", "nota_media_en_periodo",
       "materias_win3", "promo_win3", "nota_win3", "dias_desde_ult_actividad"]
DER = ["promo_rate_period", "promo_rate_win3", "materias_cum"]
FEATURES_NUM = NUM + DER
FEATURES_CAT = ["cod_carrera"]

# Horizonte de la etiqueta dropout_next: mira los 4 períodos siguientes a t
# (debe coincidir con la ventana definida en marts/student_panel.sql).
LABEL_HORIZON = 4


def shift_period(period, k):
    """Período k cuatrimestres antes (k>0) o después (k<0). Ej: shift_period('2023_1C', 4) -> '2021_1C'."""
    n = int(period[:4]) * 2 + (int(period[5]) - 1) - k
    return str(n // 2) + "_" + str(n % 2 + 1) + "C"


# Rolling-origin CON EMBARGO de maduración: para predecir el período de prueba P,
# el entrenamiento se corta en P - LABEL_HORIZON, de modo que NINGUNA etiqueta de
# entrenamiento dependa de actividad ocurrida durante o después de P. Sin este
# embargo, las etiquetas de las filas recientes de entrenamiento (que miran 4
# períodos al futuro) filtran información del período evaluado (fuga temporal).
TEST_PERIODS = ["2021_2C", "2022_1C", "2022_2C", "2023_1C"]
ORIGINS = [(shift_period(t, LABEL_HORIZON), t) for t in TEST_PERIODS]

# Experimento de dato incremental: test FIJO, corte de entrenamiento creciente
# (todos respetan el embargo: <= FIXED_TEST - LABEL_HORIZON). Aísla el efecto de
# incorporar más historia de entrenamiento sobre un mismo conjunto de prueba.
FIXED_TEST = "2023_1C"
INCREMENTAL_CUTOFFS = ["2019_2C", "2020_1C", "2020_2C", "2021_1C"]


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


def _set_style():
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        pass
    plt.rcParams.update({
        "font.family": "serif", "font.size": 11, "axes.titlesize": 12,
        "axes.labelsize": 11, "legend.fontsize": 9,
        "savefig.dpi": 300, "savefig.bbox": "tight", "savefig.pad_inches": 0.1,
    })


def plot_roc_by_period(roc_data, out_dir):
    """roc_curves_comparison.png — curva ROC del modelo en cada período de prueba."""
    fig, ax = plt.subplots(figsize=(6, 5.5))
    for i, d in enumerate(roc_data):
        fpr, tpr, _ = roc_curve(d["y"], d["p"])
        a = roc_auc_score(d["y"], d["p"])
        ax.plot(fpr, tpr, color=PERIOD_COLORS[i % len(PERIOD_COLORS)], lw=2.0,
                label=f"Prueba {d['period'].replace('_', '-')}  (AUC = {a:.3f})")
    # banda del baseline de recencia (rango de AUC entre períodos)
    base_aucs = [roc_auc_score(d["y"], d["base"]) for d in roc_data]
    ax.plot([], [], " ", label=f"Baseline recencia: AUC {min(base_aucs):.2f}–{max(base_aucs):.2f}")
    ax.plot([0, 1], [0, 1], "k--", lw=1.2, alpha=0.5, label="Aleatorio (AUC = 0.500)")
    ax.set_xlabel("Tasa de Falsos Positivos (1 − Especificidad)")
    ax.set_ylabel("Tasa de Verdaderos Positivos (Sensibilidad)")
    ax.set_title("Curvas ROC por período de prueba — backtest de origen móvil")
    ax.legend(loc="lower right", framealpha=0.9)
    ax.set_xlim(-0.01, 1.01); ax.set_ylim(-0.01, 1.01)
    path = os.path.join(out_dir, "roc_curves_comparison.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


def plot_metrics_evolution(res, out_dir):
    """metrics_comparison.png — AUC (con IC), Brier y BSS a lo largo de los orígenes."""
    x = np.arange(len(res))
    labels = [f"{o}→{t}".replace("_", "-") for o, t in zip(res["origin"], res["test_period"])]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.6))

    ax = axes[0]
    yerr = [res["auc"] - res["auc_ci_low"], res["auc_ci_high"] - res["auc"]]
    ax.errorbar(x, res["auc"], yerr=yerr, fmt="o-", color="#1565C0", lw=2,
                capsize=4, markersize=7, label="Modelo (IC 95%)")
    ax.plot(x, res["baseline_recency_auc"], "s--", color="#B71C1C", lw=1.6,
            markersize=6, label="Baseline recencia")
    ax.set_ylim(0.5, 1.0); ax.set_ylabel("ROC-AUC"); ax.set_title("AUC out-of-time")
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.legend(fontsize=8)
    for xi, v in zip(x, res["auc"]):
        ax.text(xi, v + 0.012, f"{v:.3f}", ha="center", fontsize=8.5, fontweight="bold")

    ax = axes[1]
    ax.bar(x, res["brier"], color="#2E7D32", alpha=0.85, edgecolor="white", width=0.55)
    ax.set_ylabel("Brier Score ↓"); ax.set_title("Error probabilístico (Brier)")
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=15, ha="right")
    for xi, v in zip(x, res["brier"]):
        ax.text(xi, v + 0.002, f"{v:.3f}", ha="center", fontsize=8.5, fontweight="bold")

    ax = axes[2]
    ax.bar(x, res["brier_skill_score"], color="#E65100", alpha=0.85, edgecolor="white", width=0.55)
    ax.set_ylim(0, 1); ax.set_ylabel("Brier Skill Score ↑")
    ax.set_title("Mejora vs. predictor constante")
    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=15, ha="right")
    for xi, v in zip(x, res["brier_skill_score"]):
        ax.text(xi, v + 0.01, f"{v:.2f}", ha="center", fontsize=8.5, fontweight="bold")

    fig.suptitle("Métricas del backtest de origen móvil — modelo GradientBoosting", y=1.02)
    fig.tight_layout()
    path = os.path.join(out_dir, "metrics_comparison.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


def plot_incremental(inc, out_dir):
    """incremental_training.png — AUC sobre un test FIJO al crecer el entrenamiento."""
    fig, ax = plt.subplots(figsize=(6.5, 5))
    x = np.arange(len(inc))
    yerr = [inc["auc"] - inc["auc_ci_low"], inc["auc_ci_high"] - inc["auc"]]
    ax.errorbar(x, inc["auc"], yerr=yerr, fmt="o-", color="#1565C0", lw=2,
                capsize=4, markersize=7)
    ax.set_xticks(x)
    ax.set_xticklabels([f"hasta {c}\n(n={n:,})".replace("_", "-")
                        for c, n in zip(inc["train_cutoff"], inc["n_train"])], fontsize=8)
    ax.set_ylabel(f"ROC-AUC (test fijo {inc['test_period'].iloc[0].replace('_', '-')})")
    ax.set_title("Efecto del entrenamiento incremental (test fijo, IC 95%)")
    for xi, v in zip(x, inc["auc"]):
        ax.text(xi, v + 0.004, f"{v:.3f}", ha="center", fontsize=8.5, fontweight="bold")
    fig.tight_layout()
    path = os.path.join(out_dir, "incremental_training.png")
    fig.savefig(path); plt.close(fig)
    print(f"  Guardado: {path}")


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel WHERE at_risk = 1 AND dropout_next IS NOT NULL",
        engine,
    )
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)

    rows = []
    roc_data = []
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
        roc_data.append({"period": test, "y": yte, "p": p, "base": base})

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

    # ── Experimento de dato incremental: test FIJO, corte creciente (con embargo) ──
    te_f = df[df.academic_period == FIXED_TEST]
    Xte_f, yte_f = te_f[FEATURES_NUM + FEATURES_CAT], te_f["dropout_next"].values
    leg_f = te_f["legajo"].values
    inc_rows = []
    for cut in INCREMENTAL_CUTOFFS:
        tr_f = df[df.academic_period <= cut]
        m = build_pipeline(GradientBoostingClassifier(
            n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
            tr_f[FEATURES_NUM + FEATURES_CAT], tr_f["dropout_next"].values)
        pf = m.predict_proba(Xte_f)[:, 1]
        lo_f, hi_f = grouped_bootstrap_auc(yte_f, pf, leg_f)
        inc_rows.append(dict(
            train_cutoff=cut, test_period=FIXED_TEST, n_train=len(tr_f), n_test=len(te_f),
            auc=round(roc_auc_score(yte_f, pf), 4),
            auc_ci_low=round(lo_f, 4), auc_ci_high=round(hi_f, 4),
        ))
    inc = pd.DataFrame(inc_rows)
    print(f"\nExperimento incremental (test fijo = {FIXED_TEST}):")
    print(inc.to_string(index=False))

    # ── Evaluación agrupada por legajo: generalización a estudiantes NO vistos ──────
    # El bootstrap agrupado sobre un test de un único período NO mide la generalización
    # a estudiantes nuevos (cada legajo aparece una vez). Para el origen final
    # (corte 2021_1C, test 2023_1C) se distingue:
    #   - subgrupos del test con el MISMO modelo embargado: estudiantes ya vistos en
    #     entrenamiento vs estudiantes nuevos (sin filas en el train);
    #   - una partición agrupada ESTRICTA: entrenar excluyendo del train a los legajos
    #     del test, de modo que TODOS los estudiantes del test sean no vistos.
    cut_g  = shift_period(FIXED_TEST, LABEL_HORIZON)
    tr_all = df[df.academic_period <= cut_g]
    te_g   = df[df.academic_period == FIXED_TEST]
    yg     = te_g["dropout_next"].values
    leg_g  = te_g["legajo"].values
    train_legajos = set(tr_all["legajo"].unique())
    seen = np.isin(leg_g, list(train_legajos))
    n_seen, n_new = int(seen.sum()), int((~seen).sum())

    def _safe_auc(y, p):
        return float(roc_auc_score(y, p)) if len(np.unique(y)) > 1 else float("nan")

    model_std = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr_all[FEATURES_NUM + FEATURES_CAT], tr_all["dropout_next"].values)
    pg = model_std.predict_proba(te_g[FEATURES_NUM + FEATURES_CAT])[:, 1]
    auc_seen = _safe_auc(yg[seen], pg[seen])
    auc_new  = _safe_auc(yg[~seen], pg[~seen])

    tr_grp = tr_all[~tr_all["legajo"].isin(set(leg_g))]
    model_grp = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr_grp[FEATURES_NUM + FEATURES_CAT], tr_grp["dropout_next"].values)
    p_grp = model_grp.predict_proba(te_g[FEATURES_NUM + FEATURES_CAT])[:, 1]
    lo_grp, hi_grp = grouped_bootstrap_auc(yg, p_grp, leg_g)

    grp = pd.DataFrame([dict(
        test_period=FIXED_TEST, train_cutoff=cut_g, n_test=len(te_g),
        n_test_seen=n_seen, n_test_new=n_new,
        auc_full=round(_safe_auc(yg, pg), 4), auc_seen=round(auc_seen, 4), auc_new=round(auc_new, 4),
        n_train_full=len(tr_all), n_train_grouped=len(tr_grp),
        auc_grouped=round(_safe_auc(yg, p_grp), 4),
        auc_grouped_ci_low=round(lo_grp, 4), auc_grouped_ci_high=round(hi_grp, 4),
    )])
    print(f"\nEvaluación agrupada por legajo (test fijo = {FIXED_TEST}):")
    print(grp.to_string(index=False))

    # ── Figuras para la tesis ──────────────────────────────────────────────────
    os.makedirs(THESIS_FIGS_DIR, exist_ok=True)
    _set_style()
    plot_roc_by_period(roc_data, THESIS_FIGS_DIR)
    plot_metrics_evolution(res, THESIS_FIGS_DIR)
    plot_incremental(inc, THESIS_FIGS_DIR)

    # Importancia de features del modelo del último origen (con embargo)
    imp = last_model.named_steps["model"].feature_importances_
    num_imp = list(zip(FEATURES_NUM, imp[:len(FEATURES_NUM)]))
    cat_imp = float(imp[len(FEATURES_NUM):].sum())
    print(f"\nImportancia de features (corte {ORIGINS[-1][0]} -> test {ORIGINS[-1][1]}):")
    for n, v in sorted(num_imp, key=lambda x: -x[1]):
        print(f"  {n:28s} {v:.3f}")
    print(f"  {'cod_carrera (one-hot)':28s} {cat_imp:.3f}")

    # Persistir resultados para trazabilidad / Superset
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.backtest_results"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.backtest_incremental"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.backtest_grouped"))
        conn.commit()
    res.to_sql("backtest_results", engine, schema="predictions", if_exists="replace", index=False)
    inc.to_sql("backtest_incremental", engine, schema="predictions", if_exists="replace", index=False)
    grp.to_sql("backtest_grouped", engine, schema="predictions", if_exists="replace", index=False)
    print("\nResultados persistidos en predictions.backtest_results / backtest_incremental / backtest_grouped")


if __name__ == "__main__":
    main()
