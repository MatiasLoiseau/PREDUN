"""
Utilidades de detección de drift para PREDUN.

Implementa PSI (Population Stability Index) para features continuas y categóricas.
Módulo de funciones puras: sin dependencias de Dagster ni MLflow, completamente testeable.

Umbrales PSI (estándar de la literatura de riesgo financiero y MLOps):
  PSI < 0.10  : sin cambio significativo — reentrenamiento rutinario
  0.10 – 0.25 : cambio moderado         — monitorear, aumentar frecuencia de reentrenamiento
  PSI > 0.25  : cambio severo           — revisar modelo y posiblemente arquitectura
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple

# ── Constantes ────────────────────────────────────────────────────────────────

PSI_THRESHOLD_LOW  = 0.10   # sin drift significativo
PSI_THRESHOLD_HIGH = 0.25   # drift severo
N_BINS_DEFAULT     = 10     # bins para variables continuas
EPSILON            = 1e-6   # evitar log(0) y división por cero

# Features a analizar para drift (excluye materias_cum porque es acumulativa por
# definición y siempre deriva, y cod_carrera que está acotado a 29 carreras estables)
FEATURES_NUM = [
    "materias_en_periodo",
    "promo_en_periodo",
    "nota_media_en_periodo",
    "materias_win3",
    "promo_win3",
    "nota_win3",
    "dias_desde_ult_actividad",
    "promo_rate_period",
    "promo_rate_win3",
]
FEATURES_CAT = ["cod_carrera"]
LABEL_COL    = "dropout_next"

# Corte de entrenamiento — mismo que en ml_assets.py
TRAIN_CUTOFF = "2022_2C"


# ── Funciones de PSI ──────────────────────────────────────────────────────────

def drift_level(psi: float) -> str:
    """Convierte valor PSI al nivel de drift semántico."""
    if psi < PSI_THRESHOLD_LOW:
        return "none"
    elif psi < PSI_THRESHOLD_HIGH:
        return "moderate"
    else:
        return "high"


def psi_continuous(
    reference: pd.Series,
    current: pd.Series,
    n_bins: int = N_BINS_DEFAULT,
) -> Tuple[float, pd.DataFrame]:
    """
    Calcula PSI entre dos distribuciones continuas.

    Los bins se definen por percentiles de la distribución de referencia,
    lo que garantiza que cada bin tenga aproximadamente la misma densidad
    en la referencia y hace al índice comparable entre features.

    Returns:
        psi_value: índice PSI agregado
        bin_df: detalle por bin (útil para diagnóstico)
    """
    ref = reference.dropna()
    cur = current.dropna()

    if len(ref) == 0 or len(cur) == 0:
        return 0.0, pd.DataFrame()

    # Bins por percentiles de la referencia
    edges = np.percentile(ref, np.linspace(0, 100, n_bins + 1))
    edges[0]  -= EPSILON
    edges[-1] += EPSILON
    edges = np.unique(edges)

    if len(edges) < 3:  # distribución degenerada (p.ej. constante)
        return 0.0, pd.DataFrame()

    ref_counts, _ = np.histogram(ref, bins=edges)
    cur_counts, _ = np.histogram(cur, bins=edges)

    ref_pct = np.where(ref_counts == 0, EPSILON, ref_counts / len(ref))
    cur_pct = np.where(cur_counts == 0, EPSILON, cur_counts / len(cur))

    contributions = (cur_pct - ref_pct) * np.log(cur_pct / ref_pct)
    psi_value = float(np.sum(contributions))

    bin_df = pd.DataFrame({
        "bin_low":      edges[:-1],
        "bin_high":     edges[1:],
        "ref_count":    ref_counts,
        "cur_count":    cur_counts,
        "ref_pct":      ref_pct,
        "cur_pct":      cur_pct,
        "psi_bin":      contributions,
    })

    return psi_value, bin_df


def psi_categorical(
    reference: pd.Series,
    current: pd.Series,
) -> float:
    """
    Calcula PSI para una variable categórica.

    Itera sobre la unión de categorías presentes en referencia y actual.
    Categorías nuevas (no vistas en referencia) contribuyen con su peso completo.
    """
    ref = reference.dropna()
    cur = current.dropna()

    if len(ref) == 0 or len(cur) == 0:
        return 0.0

    all_cats = set(ref.unique()) | set(cur.unique())
    ref_vc = ref.value_counts()
    cur_vc = cur.value_counts()

    psi_value = 0.0
    for cat in all_cats:
        r = max(ref_vc.get(cat, 0) / len(ref), EPSILON)
        c = max(cur_vc.get(cat, 0) / len(cur), EPSILON)
        psi_value += (c - r) * np.log(c / r)

    return float(psi_value)


# ── Función principal de análisis de drift ────────────────────────────────────

def compute_feature_drift(
    df_reference: pd.DataFrame,
    df_current: pd.DataFrame,
    features_num: Optional[List[str]] = None,
    features_cat: Optional[List[str]] = None,
    include_label: bool = True,
) -> pd.DataFrame:
    """
    Calcula PSI para todas las features entre referencia y conjunto actual.

    Args:
        df_reference: datos de referencia (distribución base — típicamente training set)
        df_current:   datos actuales (distribución a evaluar — nuevos datos o scoring set)
        features_num: lista de features continuas a analizar
        features_cat: lista de features categóricas a analizar
        include_label: si True, incluye PSI de la variable objetivo (label drift)

    Returns:
        DataFrame con columnas:
            feature_name, feature_type, psi_value, drift_level,
            mean_reference, mean_current, n_reference, n_current
        Ordenado por psi_value descendente.
    """
    num_cols = features_num if features_num is not None else FEATURES_NUM
    cat_cols = features_cat if features_cat is not None else FEATURES_CAT

    rows = []

    for col in num_cols:
        if col not in df_reference.columns or col not in df_current.columns:
            continue
        psi, _ = psi_continuous(df_reference[col], df_current[col])
        rows.append({
            "feature_name":   col,
            "feature_type":   "numeric",
            "psi_value":      round(psi, 6),
            "drift_level":    drift_level(psi),
            "mean_reference": _safe_mean(df_reference[col]),
            "mean_current":   _safe_mean(df_current[col]),
            "n_reference":    int(df_reference[col].notna().sum()),
            "n_current":      int(df_current[col].notna().sum()),
        })

    for col in cat_cols:
        if col not in df_reference.columns or col not in df_current.columns:
            continue
        psi = psi_categorical(df_reference[col], df_current[col])
        rows.append({
            "feature_name":   col,
            "feature_type":   "categorical",
            "psi_value":      round(psi, 6),
            "drift_level":    drift_level(psi),
            "mean_reference": None,
            "mean_current":   None,
            "n_reference":    int(df_reference[col].notna().sum()),
            "n_current":      int(df_current[col].notna().sum()),
        })

    if include_label and LABEL_COL in df_reference.columns and LABEL_COL in df_current.columns:
        # Label drift: compara distribución de dropout_next (binario → trato como categórico).
        # Se descartan etiquetas censuradas (NULL) antes del cast para no contar 'nan' como categoría.
        psi = psi_categorical(
            df_reference[LABEL_COL].dropna().astype(int).astype(str),
            df_current[LABEL_COL].dropna().astype(int).astype(str),
        )
        rows.append({
            "feature_name":   LABEL_COL,
            "feature_type":   "label",
            "psi_value":      round(psi, 6),
            "drift_level":    drift_level(psi),
            "mean_reference": _safe_mean(df_reference[LABEL_COL]),
            "mean_current":   _safe_mean(df_current[LABEL_COL]),
            "n_reference":    int(df_reference[LABEL_COL].notna().sum()),
            "n_current":      int(df_current[LABEL_COL].notna().sum()),
        })

    df_drift = pd.DataFrame(rows)
    if not df_drift.empty:
        df_drift = df_drift.sort_values("psi_value", ascending=False).reset_index(drop=True)

    return df_drift


def drift_summary(df_drift: pd.DataFrame) -> Dict:
    """
    Resume los resultados de drift en un dict serializable.

    Retorna el nivel de drift global (el peor feature determina el nivel),
    conteos por nivel, y el top-3 de features con mayor PSI.
    """
    if df_drift.empty:
        return {"overall_level": "unknown", "max_psi": 0.0, "n_features": 0}

    max_psi  = float(df_drift["psi_value"].max())
    n_high   = int((df_drift["drift_level"] == "high").sum())
    n_mod    = int((df_drift["drift_level"] == "moderate").sum())
    n_none   = int((df_drift["drift_level"] == "none").sum())

    if n_high > 0:
        overall = "high"
    elif n_mod > 0:
        overall = "moderate"
    else:
        overall = "none"

    top3 = (
        df_drift[["feature_name", "psi_value", "drift_level"]]
        .head(3)
        .to_dict(orient="records")
    )

    return {
        "overall_level":   overall,
        "max_psi":         round(max_psi, 6),
        "n_features_high": n_high,
        "n_features_mod":  n_mod,
        "n_features_none": n_none,
        "n_features":      len(df_drift),
        "top3_features":   top3,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_mean(series: pd.Series) -> Optional[float]:
    valid = series.dropna()
    if len(valid) == 0:
        return None
    try:
        return float(valid.mean())
    except (TypeError, ValueError):
        return None
