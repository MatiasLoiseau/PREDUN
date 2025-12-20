"""
Compare distributions between two dataset versions (2025_1C vs 2025_2C) for
students, historical course records, and progress files.

Generates a Markdown report with per-column statistics and distance metrics.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
REPORT_PATH = Path(__file__).resolve().parent / "version_comparison_report.md"


@dataclass
class DatasetPair:
    name: str
    path_a: Path
    path_b: Path


DATASETS: List[DatasetPair] = [
    DatasetPair(
        name="Alumnos",
        path_a=BASE_DIR / "data-private/2025_1C/alumnos20250514_clean.csv",
        path_b=BASE_DIR / "data-private/2025_2C/alumnos_clean.csv",
    ),
    DatasetPair(
        name="Cursada Historica",
        path_a=BASE_DIR / "data-private/2025_1C/historico_cursada.csv",
        path_b=BASE_DIR / "data-private/2025_2C/historico_cursada.csv",
    ),
    DatasetPair(
        name="Avance",
        path_a=BASE_DIR / "data-private/2025_1C/porcentaje_avance_20250512_clean.csv",
        path_b=BASE_DIR / "data-private/2025_2C/porcentaje_avance_20251112_clean.csv",
    ),
]


def infer_kind(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "categorical"


def numeric_stats(series: pd.Series) -> Dict[str, float]:
    s = pd.to_numeric(series, errors="coerce")
    return {
        "count": len(s),
        "missing_pct": float(s.isna().mean() * 100),
        "mean": float(s.mean()) if len(s.dropna()) else np.nan,
        "std": float(s.std()) if len(s.dropna()) else np.nan,
        "min": float(s.min()) if len(s.dropna()) else np.nan,
        "p25": float(s.quantile(0.25)) if len(s.dropna()) else np.nan,
        "median": float(s.median()) if len(s.dropna()) else np.nan,
        "p75": float(s.quantile(0.75)) if len(s.dropna()) else np.nan,
        "max": float(s.max()) if len(s.dropna()) else np.nan,
    }


def ks_distance(a: pd.Series, b: pd.Series) -> float:
    a_clean = pd.to_numeric(a, errors="coerce").dropna()
    b_clean = pd.to_numeric(b, errors="coerce").dropna()
    if a_clean.empty or b_clean.empty:
        return float("nan")

    a_sorted = np.sort(a_clean.to_numpy())
    b_sorted = np.sort(b_clean.to_numpy())
    combined = np.sort(np.concatenate([a_sorted, b_sorted]))

    cdf_a = np.searchsorted(a_sorted, combined, side="right") / a_sorted.size
    cdf_b = np.searchsorted(b_sorted, combined, side="right") / b_sorted.size
    return float(np.max(np.abs(cdf_a - cdf_b)))


def categorical_distance(a: pd.Series, b: pd.Series) -> Tuple[float, pd.Series, pd.Series]:
    a_counts = a.fillna("<<NA>>").value_counts(normalize=True)
    b_counts = b.fillna("<<NA>>").value_counts(normalize=True)
    categories = set(a_counts.index).union(set(b_counts.index))
    distance = 0.5 * sum(abs(a_counts.get(cat, 0.0) - b_counts.get(cat, 0.0)) for cat in categories)
    return float(distance), a_counts.head(10), b_counts.head(10)


def load_dataframe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def compare_pair(pair: DatasetPair) -> str:
    df_a = load_dataframe(pair.path_a)
    df_b = load_dataframe(pair.path_b)

    columns = sorted(set(df_a.columns).union(df_b.columns))
    rows: List[Tuple[str, str, float, float, int, int, float]] = []
    detail_blocks: List[str] = []

    for col in columns:
        series_a = df_a[col] if col in df_a else pd.Series(dtype=object)
        series_b = df_b[col] if col in df_b else pd.Series(dtype=object)

        kind = infer_kind(series_a if col in df_a else series_b)

        missing_a = float(series_a.isna().mean() * 100) if not series_a.empty else 100.0
        missing_b = float(series_b.isna().mean() * 100) if not series_b.empty else 100.0
        unique_a = int(series_a.nunique(dropna=True)) if not series_a.empty else 0
        unique_b = int(series_b.nunique(dropna=True)) if not series_b.empty else 0

        if kind == "numeric":
            distance = ks_distance(series_a, series_b)
        else:
            distance, top_a, top_b = categorical_distance(series_a, series_b)
            detail_blocks.append(_format_top_categories(col, top_a, top_b))

        rows.append((col, kind, missing_a, missing_b, unique_a, unique_b, distance))

    # Ordenar por mayor diferencia (distancia) primero; NaN al final.
    rows.sort(key=lambda r: (np.isnan(r[6]), -r[6] if not np.isnan(r[6]) else 0.0))

    lines: List[str] = []
    header = (
        "| columna | tipo | missing_1C% | missing_2C% | unique_1C | unique_2C | distancia |\n"
        "| --- | --- | --- | --- | --- | --- | --- |"
    )
    lines.append(header)

    for col, kind, missing_a, missing_b, unique_a, unique_b, distance in rows:
        lines.append(
            f"| {col} | {kind} | {missing_a:.1f} | {missing_b:.1f} | {unique_a} | {unique_b} | {distance:.4f} |"
        )

    result = [f"## {pair.name}", ""]
    result.extend(lines)
    result.append("")
    result.extend(detail_blocks)
    result.append("")
    return "\n".join(result)


def _format_top_categories(col: str, top_a: pd.Series, top_b: pd.Series) -> str:
    lines = [f"**Top categorias para {col}**"]
    lines.append("")
    lines.append("| valor | 2025_1C % | 2025_2C % | diferencia pp |")
    lines.append("| --- | --- | --- | --- |")

    categories = set(top_a.index).union(set(top_b.index))
    # Ordenar por mayor diferencia absoluta de proporciones.
    sorted_cats = sorted(
        categories,
        key=lambda cat: abs(top_b.get(cat, 0.0) - top_a.get(cat, 0.0)),
        reverse=True,
    )

    for cat in sorted_cats[:10]:
        pct_a = top_a.get(cat, 0.0) * 100
        pct_b = top_b.get(cat, 0.0) * 100
        diff = pct_b - pct_a
        lines.append("| {cat} | {a:.2f} | {b:.2f} | {d:.2f} |".format(cat=str(cat), a=pct_a, b=pct_b, d=diff))
    lines.append("")
    return "\n".join(lines)


def generate_report(pairs: Iterable[DatasetPair], output_path: Path) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sections = ["# Comparacion de versiones 2025_1C vs 2025_2C", f"Generado: {timestamp}", ""]

    for pair in pairs:
        sections.append(compare_pair(pair))

    output_path.write_text("\n".join(sections), encoding="utf-8")


def main() -> None:
    generate_report(DATASETS, REPORT_PATH)
    print(f"Reporte guardado en {REPORT_PATH}")


if __name__ == "__main__":
    main()
