"""
Efecto del período de borde sobre las etiquetas de validación (dictamen G5).

En la entrega 2025_1C el período 2025_1C estaba casi vacío, y la etiqueta de
2023_1C lo cuenta como observado. Se toma el mismo modelo y las mismas variables
de 2023_1C y se lo evalúa con las etiquetas de cada entrega (recon.panel_*).

Uso:
    conda run -n eda-predun python scripts/borde_etiquetas_validacion.py
"""
import warnings

import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from backtest_temporal import PG_URI, FEATURES_NUM, FEATURES_CAT, build_pipeline

warnings.filterwarnings("ignore")

TEST_PERIOD = "2023_1C"
TRAIN_CUTOFF = "2021_1C"
KEY = ["legajo", "cod_carrera", "academic_period"]
F = FEATURES_NUM + FEATURES_CAT


def period_ord(p):
    return p.str[:4].astype(int) * 2 + p.str[5].astype(int)


def main():
    eng = create_engine(PG_URI)
    panels = {e: pd.read_sql(f"SELECT * FROM recon.panel_{e} "
                             "WHERE at_risk = 1 AND dropout_next IS NOT NULL", eng)
              for e in ("2025_1c", "2025_2c")}

    # Variables de la entrega final y etiquetas de ambas entregas, sobre las mismas filas.
    test = panels["2025_2c"].query("academic_period == @TEST_PERIOD")
    lab_1c = panels["2025_1c"].query("academic_period == @TEST_PERIOD")[KEY + ["dropout_next"]]
    test = test.merge(lab_1c, on=KEY, suffixes=("", "_1c"))
    y_2c, y_1c = test["dropout_next"].astype(int), test["dropout_next_1c"].astype(int)
    print(f"filas {len(test)} | etiquetas distintas {(y_2c != y_1c).sum()} "
          f"| de 1 a 0 {((y_1c == 1) & (y_2c == 0)).sum()}")

    cut = period_ord(pd.Series([TRAIN_CUTOFF]))[0]
    for entrega, pan in panels.items():
        tr = pan[period_ord(pan["academic_period"]) <= cut]
        clf = build_pipeline(GradientBoostingClassifier(
            n_estimators=100, max_depth=3, subsample=0.8, random_state=42))
        clf.fit(tr[F], tr["dropout_next"].astype(int))
        p = clf.predict_proba(test[F])[:, 1]
        print(f"modelo entrenado con la entrega {entrega} (n={len(tr)}) | "
              f"AUC etiquetas 2025_1C {roc_auc_score(y_1c, p):.4f} | "
              f"AUC etiquetas 2025_2C {roc_auc_score(y_2c, p):.4f}")


if __name__ == "__main__":
    main()
