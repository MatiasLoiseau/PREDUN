"""
(C) Sensibilidad al proxy de finalización curricular estimada (avance >= 90%).

Responde a la observación del jurado (Q3 / prioridad 1.2): la finalización
curricular se estima con una FOTOGRAFÍA ÚNICA del avance curricular (no
historizada), por lo que la exclusión por finalización estimada no es
estrictamente punto en el tiempo y podría suprimir retrospectivamente etiquetas
de abandono. Se cuantifica el efecto comparando dos definiciones de la etiqueta
dropout_next:

  - CON exclusión por finalización estimada (definición actual del sistema): un
    legajo con avance >= 90% nunca recibe etiqueta de abandono.
  - SIN exclusión: la etiqueta se decide solo por la actividad futura.

Se reconstruyen ambas etiquetas a partir de la grilla de actividad por período
(idéntica lógica de ventana que marts/student_panel.sql: censura estricta a 4
períodos), se re-entrena el modelo de referencia (corte 2021_1C, prueba 2023_1C)
con cada una y se reportan filas, prevalencia, AUC, Brier y Precision@10, además
del número de filas y estudiantes reclasificados.

Uso:
    conda run -n eda-predun python scripts/sensitivity_graduation.py
"""
import os
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, brier_score_loss

from backtest_temporal import PG_URI, build_pipeline, FEATURES_NUM, FEATURES_CAT, shift_period, LABEL_HORIZON
from sensitivity_horizon import pidx_from_row

warnings.filterwarnings("ignore")

TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C
H = LABEL_HORIZON


def period_to_pidx(label):
    return int(label[:4]) * 2 + (0 if label[5] == "1" else 1)


def p_at_k(y, p, k=0.10):
    order = np.argsort(-p)
    return float(np.asarray(y)[order[:max(int(len(p) * k), 1)]].mean())


def evaluate(df, label_col):
    sub = df[(df.at_risk == 1) & df[label_col].notna()].copy()
    sub[label_col] = sub[label_col].astype(int)
    tr = sub[sub.academic_period <= TRAIN_CUTOFF]
    te = sub[sub.academic_period == TEST_PERIOD]
    m = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr[FEATURES_NUM + FEATURES_CAT], tr[label_col].values)
    p = m.predict_proba(te[FEATURES_NUM + FEATURES_CAT])[:, 1]
    y = te[label_col].values
    return dict(
        etiqueta=label_col, n_filas_etiquetadas=len(sub), n_train=len(tr), n_test=len(te),
        prevalencia_global=round(float(sub[label_col].mean()), 4),
        prevalencia_test=round(float(y.mean()), 4),
        auc=round(roc_auc_score(y, p), 4), brier=round(brier_score_loss(y, p), 4),
        p_at_10=round(p_at_k(y, p), 4),
    )


def main():
    engine = create_engine(PG_URI)

    # Actividad por período (mismo criterio de fecha que student_panel.sql)
    act = pd.read_sql(text("""
        SELECT legajo,
               extract(year  from trim(fecha)::date)::int AS anio,
               extract(month from trim(fecha)::date)::int AS mes
        FROM canonical.cursada_historica
        WHERE trim(coalesce(fecha,'')) ~ '^\\d{4}-\\d{2}-\\d{2}$'
    """), engine)
    act["pidx"] = [pidx_from_row(a, m) for a, m in zip(act["anio"], act["mes"])]
    act["legajo"] = act["legajo"].astype(str)
    data_max = int(act["pidx"].max())
    active_by_leg = {lg: set(s["pidx"].unique()) for lg, s in act.groupby("legajo")}

    grad = pd.read_sql(text("""
        SELECT DISTINCT legajo FROM canonical.porcentaje_avance
        WHERE coalesce(
            CASE WHEN trim(porcentaje_avance) ~ '^\\d+([.,]\\d+)?$'
                 THEN replace(trim(porcentaje_avance), ',', '.')::numeric ELSE NULL END, 0) >= 90
    """), engine)
    finalizacion_estimada = set(grad["legajo"].astype(str))

    # Panel con features (la etiqueta dropout_next del sistema = CON exclusión)
    df = pd.read_sql("SELECT * FROM marts.student_panel", engine)
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["legajo"] = df["legajo"].astype(str)
    df["pidx"] = df["academic_period"].map(period_to_pidx)

    # Recomputar ambas etiquetas desde la grilla de actividad
    lab_grad, lab_nograd = [], []
    for legajo, t in zip(df["legajo"].values, df["pidx"].values):
        acts = active_by_leg.get(legajo, set())
        n_fut = min(H, data_max - t)
        if n_fut < H:
            lab_grad.append(np.nan); lab_nograd.append(np.nan); continue
        max_fut = 1 if any((t + k) in acts for k in range(1, H + 1)) else 0
        if max_fut == 1:
            lab_grad.append(0); lab_nograd.append(0)
        else:
            lab_nograd.append(1)
            lab_grad.append(0 if legajo in finalizacion_estimada else 1)
    df["lab_con_grad"] = lab_grad
    df["lab_sin_grad"] = lab_nograd

    # Sanidad: la etiqueta recomputada CON exclusión debe coincidir con dropout_next del panel
    chk = df[(df.at_risk == 1) & df["dropout_next"].notna() & df["lab_con_grad"].notna()]
    match = (chk["dropout_next"].astype(int).values == chk["lab_con_grad"].astype(int).values).mean()
    print(f"Verificación: etiqueta recomputada (con exclusión) coincide con el panel en "
          f"{match*100:.2f}% de {len(chk):,} filas\n")

    r_con = evaluate(df, "lab_con_grad")
    r_sin = evaluate(df, "lab_sin_grad")
    res = pd.DataFrame([r_con, r_sin])
    pd.set_option("display.width", 220, "display.max_columns", 30)
    print("=== (C) Sensibilidad al proxy de finalización curricular estimada ===")
    print(res.to_string(index=False))

    # Reclasificación: filas/estudiantes que pasan de 0 (finalización estimada) a 1 (abandono)
    both = df[(df.at_risk == 1) & df["lab_con_grad"].notna()]
    flipped = both[(both["lab_con_grad"] == 0) & (both["lab_sin_grad"] == 1)]
    print(f"\nFilas reclasificadas (0→1 al quitar la exclusión por finalización estimada): "
          f"{len(flipped):,} de {len(both):,} ({len(flipped)/len(both)*100:.2f}%)")
    print(f"Estudiantes (legajos) afectados: {flipped['legajo'].nunique():,}")
    d_auc = r_sin["auc"] - r_con["auc"]
    d_prev = r_sin["prevalencia_test"] - r_con["prevalencia_test"]
    print(f"\nΔ AUC (sin − con) = {d_auc:+.4f} | Δ prevalencia test = {d_prev:+.4f}")
    print("Interpretación: una diferencia pequeña ACOTA empíricamente el sesgo "
          "introducido por el proxy de finalización curricular estimada.")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.sensitivity_graduation"))
        conn.commit()
    res.to_sql("sensitivity_graduation", engine, schema="predictions", if_exists="replace", index=False)
    print("\nPersistido en predictions.sensitivity_graduation")


if __name__ == "__main__":
    main()
