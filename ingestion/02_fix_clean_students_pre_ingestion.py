#!/usr/bin/env python3
"""
Flexible Student Data Ingestion (metadata-driven)

This script turns heterogeneous CSV/TXT files into a harmonised,
de-identified CSV using nothing but the rules contained in a YAML file.

• Sensitive columns (names, document numbers, phones, e-mails, …) are
  dropped by listing them under  columns.drop  in the YAML.
• Column names are standardised through  columns.rename.
• Additional mappings, type casts, null normalisation and column order
  are likewise declarative.
"""
from __future__ import annotations

import sys
import logging
import pathlib
import yaml
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s • %(levelname)s • %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def load_config(cfg_path: str) -> dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_raw(cfg: dict) -> pd.DataFrame:
    """Reads the raw file exactly once, applying only very light cleaning:
       * removes unnamed columns automatically
       * keeps rows whose length equals expected_columns when that is set"""
    inp = cfg["input"]
    path = pathlib.Path(inp["path"]).expanduser()

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    read_kwargs = dict(
        sep=inp.get("delimiter", ","),
        encoding=inp.get("encoding", "utf-8"),
        header=0 if inp.get("header_in_file", True) else None,
    )

    df = pd.read_csv(path, **read_kwargs)

    # If header not present, assign provided source columns
    if not inp.get("header_in_file", True):
        df.columns = cfg["columns"]["source"]

    # Drop unnamed pandas auto-generated columns
    df = df.loc[:, ~df.columns.astype(str).str.match(r"Unnamed")]

    # Optionally filter malformed rows
    exp_cols = inp.get("expected_columns")
    if exp_cols:
        df = df[df.shape[1] == exp_cols]

    logging.info("%s rows read from %s", len(df), path)
    return df

def transform(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    NULL_LIKE = cfg.get("null_values", ["NULL", "null", "", "NaN"])

    # 1. rename --------------------------------------------------------
    rename_map = cfg["columns"].get("rename", {})
    df = df.rename(columns=rename_map)

    # 2. drop columns (explicit + duplicates + unnamed), always ignore errors
    drop_cols = set(cfg["columns"].get("drop", []))
    drop_cols.update(c for c in df.columns if "Unnamed" in str(c))
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # 3. normalise null semantics
    df.replace(NULL_LIKE, np.nan, inplace=True)

    # 4. value mappings -----------------------------------------------
    for col, mapping in cfg.get("value_mappings", {}).items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(df[col])

    # 5. type casting --------------------------------------------------
    for col, dtype in cfg.get("types", {}).items():
        if col in df.columns:
            if dtype in {"float", "int"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")

    # 6. final column order (if provided)
    target_cols = cfg["columns"].get("target")
    if target_cols:
        # Add any missing columns so output schema is always identical
        for col in target_cols:
            if col not in df.columns:
                df[col] = np.nan
        df = df[target_cols]

    return df

def write_out(df: pd.DataFrame, cfg: dict) -> None:
    out = cfg["output"]
    path = pathlib.Path(out["path"]).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        path,
        sep=out.get("delimiter", ","),
        encoding=out.get("encoding", "utf-8"),
        index=False,
    )
    logging.info("File saved to %s • %s rows", path, len(df))

def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("Usage: python 02_fix_clean_students_pre_ingestion.py <config.yml>")

    cfg_path = sys.argv[1]
    cfg = load_config(cfg_path)

    df = read_raw(cfg)
    df = transform(df, cfg)
    write_out(df, cfg)

if __name__ == "__main__":
    main()