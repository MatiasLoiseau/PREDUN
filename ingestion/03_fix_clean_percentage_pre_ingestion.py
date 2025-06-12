#!/usr/bin/env python3
"""
Flexible Percentage Progress Ingestion (v2)

Usage
-----
    python ingestion/ingest_porcentaje.py <config.yml>

Steps
-----
1. Loads YAML configuration.
2. Reads the source CSV file.
3. Drops sensitive or explicitly excluded columns.
4. Applies renaming, value mappings, and type conversions.
5. Ensures the target schema is present and correctly ordered.
6. Writes the cleaned DataFrame to disk.
"""

import sys
import logging
import pathlib
from typing import Dict, List, Any

import yaml
import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s • %(levelname)s • %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --------------------------------------------------------------------------- #
# Columns that must NEVER appear in the final output (PII)
# --------------------------------------------------------------------------- #
SENSITIVE_COLS = {"apellido", "nombres", "nro_documento", "tipo_documento"}

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def load_config(path: str) -> Dict[str, Any]:
    """Load YAML file from disk."""
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def read_raw(cfg: Dict[str, Any]) -> pd.DataFrame:
    """Load the raw CSV file based on the input section of the YAML."""
    meta = cfg["input"]
    path = pathlib.Path(meta["path"])

    if not path.exists():
        raise FileNotFoundError(path)

    read_kwargs = dict(
        sep=meta.get("delimiter", ","),
        encoding=meta.get("encoding", "utf-8"),
        dtype=str,
        keep_default_na=False,
        quoting=0,
    )

    header = 0 if meta.get("header_in_file", False) else None
    if header is None:
        read_kwargs["names"] = cfg["columns"]["source"]

    df = pd.read_csv(path, header=header, **read_kwargs)

    exp_cols = meta.get("expected_columns")
    if exp_cols and df.shape[1] != exp_cols:
        raise ValueError(
            f"{path}: Expected {exp_cols} columns, but got {df.shape[1]}"
        )

    logging.info("Loaded %s records from %s", len(df), path)
    return df


def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any PII (personally identifiable information) fields."""
    drop = [c for c in df.columns if c.lower() in SENSITIVE_COLS]
    return df.drop(columns=drop, errors="ignore")


def transform(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Apply column drops, renaming, value mappings, type casts, and enforce schema."""
    col_cfg = cfg["columns"]

    # Step 1: Drop explicitly listed columns
    df = df.drop(columns=col_cfg.get("drop", []), errors="ignore")

    # Step 2: Force-drop sensitive columns
    df = sanitize(df)

    # Step 3: Rename columns
    df = df.rename(columns=col_cfg.get("rename", {}))

    # Step 4: Map values (normalization)
    for col, mapping in cfg.get("value_mappings", {}).items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(df[col])

    # Step 5: Replace null-like values and cast types
    nulls = set(cfg.get("null_values", ["NULL", "null", ""]))
    df.replace(list(nulls), np.nan, inplace=True)

    for col, dtype in cfg.get("types", {}).items():
        if col not in df.columns:
            continue
        if dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").dropna().astype(int)
        else:
            df[col] = df[col].astype(dtype)

    # Step 6: Enforce canonical schema
    target_cols: List[str] = col_cfg["target"]
    for missing in (set(target_cols) - set(df.columns)):
        df[missing] = np.nan
    df = df[target_cols]  # reorder

    return df


def write_out(df: pd.DataFrame, cfg: Dict[str, Any]) -> None:
    """Write the cleaned DataFrame to a CSV."""
    meta = cfg["output"]
    path = pathlib.Path(meta["path"])
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        path,
        sep=meta.get("delimiter", ","),
        encoding=meta.get("encoding", "utf-8"),
        index=False,
    )
    logging.info("Saved %s records to %s", len(df), path)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("Usage: python ingest_porcentaje.py <config.yml>")

    cfg = load_config(sys.argv[1])
    df = read_raw(cfg)
    df = transform(df, cfg)
    write_out(df, cfg)


if __name__ == "__main__":
    main()