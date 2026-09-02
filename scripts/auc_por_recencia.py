"""
(G) Desempeño del modelo DENTRO de cada nivel de inactividad.

Responde a la objeción de fondo sobre el AUC global: la etiqueta se define por
cuatro períodos seguidos sin actividad y `dias_desde_ult_actividad` dice cuántos
de esos períodos ya transcurrieron, así que parte del AUC global podría venir de
separar grupos que la propia definición ya separa de antemano.

Se particiona el conjunto de prueba por el valor de `dias_desde_ult_actividad`
en t. Dentro del conjunto de riesgo esa variable solo puede tomar CUATRO valores,
porque al cuarto cuatrimestre sin actividad el legajo sale del risk set. Las
métricas se calculan dentro de cada grupo con UN SOLO modelo entrenado: no se
reentrena por grupo.

Los valores absolutos en días dependen del período de prueba (los saltos entre
cuatrimestres alternan 181 y 184 días, y los bisiestos corren un día), por eso
los grupos se etiquetan por posición y no por el valor literal.

Protocolo: corte de entrenamiento 2021_1C, prueba 2023_1C (embargo de 4
períodos), idéntico al modelo de referencia de la tesis.

Genera la Tabla `tab:auc_por_recencia` del capítulo 5.

Uso:
    conda run -n eda-predun python scripts/auc_por_recencia.py
"""
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from backtest_temporal import (
    PG_URI, build_pipeline, FEATURES_NUM, FEATURES_CAT, shift_period, LABEL_HORIZON,
)

warnings.filterwarnings("ignore")

TEST_PERIOD  = "2023_1C"
TRAIN_CUTOFF = shift_period(TEST_PERIOD, LABEL_HORIZON)   # 2021_1C

# Etiquetas por posición: el grupo i-ésimo corresponde a i cuatrimestres inactivos.
GROUP_LABELS = [
    "Cursó en el período",
    "1 cuatrimestre inactivo",
    "2 cuatrimestres inactivos",
    "3 cuatrimestres inactivos",
]


def p_at_k(y, p, k=0.10):
    """Precision@K dentro del grupo: el top-K% se toma sobre ESE grupo, no sobre el total."""
    order = np.argsort(-np.asarray(p))
    return float(np.asarray(y)[order[:max(int(len(p) * k), 1)]].mean())


def main():
    engine = create_engine(PG_URI)
    df = pd.read_sql(
        "SELECT * FROM marts.student_panel "
        "WHERE at_risk = 1 AND dropout_next IS NOT NULL "
        "ORDER BY legajo, academic_period",
        engine,
    )
    df[FEATURES_NUM] = df[FEATURES_NUM].apply(pd.to_numeric, errors="coerce")
    df["dropout_next"] = df["dropout_next"].astype(int)

    tr = df[df.academic_period <= TRAIN_CUTOFF]
    te = df[df.academic_period == TEST_PERIOD].copy()
    print(f"Entrenamiento <= {TRAIN_CUTOFF}: {len(tr):,} filas | "
          f"Prueba {TEST_PERIOD}: {len(te):,} filas (prevalencia {te['dropout_next'].mean():.4f})\n")

    # Un solo modelo, el de referencia de la tesis.
    model = build_pipeline(GradientBoostingClassifier(
        n_estimators=100, max_depth=3, subsample=0.8, random_state=42)).fit(
        tr[FEATURES_NUM + FEATURES_CAT], tr["dropout_next"].values)
    te["p"] = model.predict_proba(te[FEATURES_NUM + FEATURES_CAT])[:, 1]

    auc_global   = roc_auc_score(te["dropout_next"].values, te["p"].values)
    p_at_10_glob = p_at_k(te["dropout_next"].values, te["p"].values)

    niveles = sorted(te["dias_desde_ult_actividad"].dropna().unique())
    if len(niveles) != 4:
        print(f"AVISO: se esperaban 4 niveles de inactividad y se encontraron "
              f"{len(niveles)} -> {niveles}")

    rows = []
    for i, dias in enumerate(niveles):
        g = te[te["dias_desde_ult_actividad"] == dias]
        y, p = g["dropout_next"].values, g["p"].values
        prev = float(y.mean())
        pk   = p_at_k(y, p)
        rows.append(dict(
            situacion   = GROUP_LABELS[i] if i < len(GROUP_LABELS) else f"nivel {i}",
            dias        = int(dias),
            n           = len(g),
            n_positivos = int(y.sum()),
            n_negativos = int((1 - y).sum()),
            prevalencia = round(prev, 4),
            auc         = round(float(roc_auc_score(y, p)), 4) if len(np.unique(y)) > 1 else float("nan"),
            p_at_10     = round(pk, 4),
            n_top_10    = max(int(len(g) * 0.10), 1),
            lift        = round(pk / prev, 2) if prev > 0 else float("nan"),
        ))

    res = pd.DataFrame(rows)
    pd.set_option("display.width", 220, "display.max_columns", 30)
    print("=== (G) Desempeño dentro de cada nivel de inactividad "
          f"(test {TEST_PERIOD}, corte {TRAIN_CUTOFF}) ===")
    print(res.to_string(index=False))

    # Chequeos de consistencia contra las métricas globales del período.
    print(f"\nAUC global del período      : {auc_global:.4f}")
    print(f"Precision@10% global        : {p_at_10_glob:.4f}")
    print(f"Suma de N por grupo         : {res['n'].sum():,} (esperado {len(te):,})")
    prev_pond = (res["n"] * res["prevalencia"]).sum() / res["n"].sum()
    print(f"Prevalencia ponderada       : {prev_pond:.4f} "
          f"(esperado {te['dropout_next'].mean():.4f})")
    print(f"AUC global > max(AUC grupo) : {auc_global > res['auc'].max()} "
          f"(max grupo = {res['auc'].max():.4f})")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
        conn.execute(text("DROP TABLE IF EXISTS predictions.auc_por_recencia"))
        conn.commit()
    res.to_sql("auc_por_recencia", engine, schema="predictions",
               if_exists="replace", index=False)
    print("\nPersistido en predictions.auc_por_recencia")


if __name__ == "__main__":
    main()
