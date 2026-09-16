"""
Curva de anticipación (lead time): cuánta exactitud se paga por adelantar la alerta.

El Capítulo 2 presenta el compromiso entre anticipación y exactitud
(earliness--accuracy trade-off) como el concepto que define la predicción
temprana, pero todo el desempeño de la tesis se reporta a horizonte fijo. Este
script mide ese compromiso sobre los datos propios.

Diseño. Se fija la población y se fija la etiqueta, y lo único que cambia es la
antigüedad de la información:

  - Población: las filas en riesgo con etiqueta observable del período de prueba
    (2023_1C). Para que las cifras sean comparables entre sí, la curva principal
    se restringe a los legajos que tienen fila en el panel en TODOS los rezagos.
  - Etiqueta: dropout_next del período de prueba, la misma en todos los rezagos
    (no cursa en ninguno de los 4 períodos siguientes).
  - Variables: en el rezago k, las diez variables del modelo se toman de la fila
    del MISMO legajo k períodos antes. Con k=0 es el modelo de la tesis.

Cada rezago se entrena con su propia estructura (features de p-k, etiqueta de p,
filas de entrenamiento hasta 2021_1C), así que el modelo aprende con el mismo
desfasaje con el que después se lo evalúa. El embargo de maduración se mantiene.

Lectura operativa: con rezago k la alerta se emite al cierre del período t-k,
o sea k cuatrimestres antes que en el sistema actual, sobre el mismo desenlace.

Uso:
    conda run -n eda-predun python scripts/lead_time.py
"""
import warnings

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from sklearn.metrics import roc_auc_score

from backtest_temporal import (
    PG_URI, THESIS_FIGS_DIR, FEATURES_NUM, FEATURES_CAT, shift_period, LABEL_HORIZON,
)
from ablation_and_paired import build, gbm, p_at_k

warnings.filterwarnings("ignore")

TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C
MAX_LAG = 3
FEATURES = FEATURES_NUM + FEATURES_CAT


def period_index(s):
    """'2023_1C' -> entero ordinal de cuatrimestre, para desplazar por rezago."""
    return s.str[:4].astype(int) * 2 + (s.str[5].astype(int) - 1)


def main():
    engine = create_engine(PG_URI)
    panel = pd.read_sql(
        f"SELECT legajo, academic_period, at_risk, dropout_next, "
        f"{', '.join(FEATURES)} FROM marts.student_panel",
        engine,
    )
    panel["pidx"] = period_index(panel["academic_period"])
    panel[FEATURES_NUM] = panel[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    # El SELECT del panel no lleva ORDER BY y el subsample del GBM consume el orden
    # físico de las filas. Se ordena acá para que la curva sea reproducible.
    panel = panel.sort_values(["legajo", "pidx"], kind="mergesort").reset_index(drop=True)

    # Filas que aportan etiqueta (población de modelado) y filas que aportan variables.
    labels = panel.loc[panel.at_risk.eq(1) & panel.dropout_next.notna(),
                       ["legajo", "pidx", "academic_period", "dropout_next"]].copy()
    labels["dropout_next"] = labels["dropout_next"].astype(int)
    feats = panel[["legajo", "pidx"] + FEATURES]

    test_idx = period_index(pd.Series([TEST_PERIOD]))[0]
    cutoff_idx = period_index(pd.Series([TRAIN_CUTOFF]))[0]

    # Cohorte comparable: legajos en riesgo en el período de prueba que además
    # tienen fila en el panel en los MAX_LAG períodos anteriores.
    te0 = labels[labels.pidx == test_idx]
    present = feats[feats.pidx.between(test_idx - MAX_LAG, test_idx)]
    n_per_legajo = present.groupby("legajo").size()
    cohorte = set(n_per_legajo[n_per_legajo == MAX_LAG + 1].index) & set(te0.legajo)
    print(f"Prueba {TEST_PERIOD}: {len(te0):,} filas en riesgo con etiqueta | "
          f"cohorte comparable (con fila en los {MAX_LAG + 1} rezagos): {len(cohorte):,} "
          f"({len(cohorte) / len(te0):.1%})\n")

    rows = []
    for k in range(MAX_LAG + 1):
        # Variables de p-k pegadas a la etiqueta de p.
        f = feats.copy()
        f["pidx"] = f["pidx"] + k
        d = labels.merge(f, on=["legajo", "pidx"], how="inner", validate="1:1")

        # El corte de entrenamiento queda fijo en TRAIN_CUTOFF para todos los rezagos,
        # que es el protocolo de referencia de la tesis. Así lo único que cambia entre
        # un punto y otro de la curva es la antigüedad de las variables.
        tr = d[d.pidx <= cutoff_idx]
        te_all = d[d.pidx == test_idx]
        te = te_all[te_all.legajo.isin(cohorte)]
        y = te["dropout_next"].values

        m = build(FEATURES_NUM, FEATURES_CAT, gbm()).fit(
            tr[FEATURES], tr["dropout_next"].values)
        p = m.predict_proba(te[FEATURES])[:, 1]

        # Baseline operativo con la misma antigüedad de información.
        base = te["dias_desde_ult_actividad"].fillna(
            te["dias_desde_ult_actividad"].median()).values.astype(float)

        # Control: AUC sin restringir a la cohorte comparable. Con rezago 0 tiene que
        # dar el 0,930 que la tesis reporta sobre las 23.685 filas de 2023_1C.
        p_all = m.predict_proba(te_all[FEATURES])[:, 1]

        rows.append(dict(
            rezago=k,
            periodo_variables=shift_period(TEST_PERIOD, k),
            n_entrenamiento=len(tr),
            n_prueba=len(te),
            prevalencia=round(float(y.mean()), 4),
            auc=round(roc_auc_score(y, p), 4),
            auc_baseline=round(roc_auc_score(y, base), 4),
            p_at_20=round(p_at_k(y, p, 0.20), 4),
            n_sin_restringir=len(te_all),
            auc_sin_restringir=round(roc_auc_score(te_all["dropout_next"].values, p_all), 4),
        ))

    out = pd.DataFrame(rows)
    pd.set_option("display.width", 200, "display.max_columns", 30)
    print("=== Curva de anticipación (misma población, misma etiqueta) ===")
    print(out.to_string(index=False))
    caida = out.auc.iloc[0] - out.auc.iloc[-1]
    print(f"\nAdelantar la alerta {MAX_LAG} cuatrimestres cuesta {caida:.3f} de AUC "
          f"({out.auc.iloc[0]:.3f} -> {out.auc.iloc[-1]:.3f}). El baseline de recencia "
          f"cae {out.auc_baseline.iloc[0] - out.auc_baseline.iloc[-1]:.3f} "
          f"({out.auc_baseline.iloc[0]:.3f} -> {out.auc_baseline.iloc[-1]:.3f}).")

    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(out.rezago, out.auc, "o-", color="#1565C0", lw=2, label="Modelo (GBM)")
    ax.plot(out.rezago, out.auc_baseline, "s--", color="#E65100", lw=2,
            label="Baseline de recencia")
    for _, r in out.iterrows():
        ax.annotate(f"{r.auc:.3f}", (r.rezago, r.auc), textcoords="offset points",
                    xytext=(0, 9), ha="center", fontsize=9, color="#1565C0")
        ax.annotate(f"{r.auc_baseline:.3f}", (r.rezago, r.auc_baseline),
                    textcoords="offset points", xytext=(0, -15), ha="center",
                    fontsize=9, color="#E65100")
    ax.set_xticks(out.rezago)
    ax.set_xticklabels([f"{k}\n({p})" for k, p in zip(out.rezago, out.periodo_variables)])
    ax.set_xlabel("Cuatrimestres de anticipación (período del que se toman las variables)")
    ax.set_ylabel("AUC sobre la prueba 2023_1C")
    ax.axhline(0.5, color="#757575", lw=1, ls=":")
    ax.annotate("azar", (1.5, 0.513), fontsize=8, color="#757575", va="bottom")
    ax.set_ylim(0.45, 0.97)
    ax.grid(alpha=0.3)
    ax.legend(loc="lower left")
    fig.tight_layout()
    path = f"{THESIS_FIGS_DIR}/lead_time.png"
    fig.savefig(path, dpi=150)
    print(f"\nFigura: {path}")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.lead_time_curve"))
        conn.commit()
    out.to_sql("lead_time_curve", engine, schema="predictions",
               if_exists="replace", index=False)
    print("Persistido en predictions.lead_time_curve")


if __name__ == "__main__":
    main()
