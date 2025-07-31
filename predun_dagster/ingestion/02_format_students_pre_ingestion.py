#!/usr/bin/env python3
"""
Flexible Student Data Ingestion (metadata-driven)

Usage
-----
    python ingestion/ingest_students.py <config.yml>

The script

1. Loads a YAML configuration that describes the raw file.
2. Reads the raw data (TXT/CSV, any delimiter, optional header).
3. Drops sensitive or unwanted columns.
4. Applies renames, value mappings and type casts.
5. Adds missing columns (filled with NaN) and re-orders them
   so every version ends with the **same canonical schema**.
6. Writes the cleaned data set to the CSV location defined
   in the YAML file.
"""

import sys
import logging
import pathlib
from typing import Dict, List, Any
import yaml
import numpy as np
import pandas as pd

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Columns that must **never** reach the final dataset
SENSITIVE_COLS = {
    "apellido",
    "nombres",
    "nro_documento",
    "telefono_numero",
    "telefono",
    "email",
}

# Helper functions
def load_config(cfg_path: str) -> Dict[str, Any]:
    """Load YAML configuration from disk."""
    with open(cfg_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def normalize_dates(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            original = df[col]
            parsed1 = pd.to_datetime(original, format="%d/%m/%Y", errors="coerce")
            parsed2 = pd.to_datetime(original, format="%Y-%m-%d", errors="coerce")
            df[col] = parsed1.combine_first(parsed2)
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df

def read_raw(cfg: Dict[str, Any]) -> pd.DataFrame:
    """Read the raw file according to the metadata in `cfg`."""
    inp = cfg["input"]
    path = pathlib.Path(inp["path"])

    if not path.exists():
        raise FileNotFoundError(f"File does not exist: {path}")

    # Choose pandas reader based on extension
    read_kwargs = {
        "sep": inp.get("delimiter", ","),
        "encoding": inp.get("encoding", "utf-8"),
        "dtype": str,  # keep all as string initially
        "quoting": 0,  # leave quoting untouched
        "keep_default_na": False,
    }

    header = 0 if inp.get("header_in_file", False) else None
    if header is None:
        read_kwargs["names"] = cfg["columns"]["source"]

    if path.suffix.lower() in {".txt", ".csv"}:
        df = pd.read_csv(path, header=header, **read_kwargs)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    expected_cols = inp.get("expected_columns")
    if expected_cols and df.shape[1] != expected_cols:
        raise ValueError(f"Expected {expected_cols} columns, but got {df.shape[1]} in file {inp['path']}")

    logging.info("Loaded %s rows from %s", len(df), path)
    return df

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop any column that is considered sensitive, even if the user
    forgot to list it in `drop`.
    """
    cols_to_drop = [c for c in df.columns if c.lower() in SENSITIVE_COLS]
    return df.drop(columns=cols_to_drop, errors="ignore")

def transform(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Apply renames, drops, value mappings and type casts."""
    col_cfg = cfg["columns"]

    # 1. Drop columns requested by the user
    raw_drop = col_cfg.get("drop", [])
    if isinstance(raw_drop, dict):
        raise ValueError(f"Expected list for 'columns.drop', got dict: {raw_drop}")
    elif isinstance(raw_drop, list):
        df = df.drop(columns=raw_drop, errors="ignore")
    else:
        raise ValueError(f"Unexpected type for 'columns.drop': {type(raw_drop)}")

    # 2. Force-drop sensitive columns
    df = sanitize(df)

    # 3. Rename to canonical names
    df = df.rename(columns=col_cfg.get("rename", {}))

    # 4. Map values (normalisations)
    for col, mapping in cfg.get("value_mappings", {}).items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(df[col])

    # 5. Cast types
    null_like = set(cfg.get("null_values", ["NULL", "null", ""]))
    df.replace(list(null_like), np.nan, inplace=True)

    for col, dtype in cfg.get("types", {}).items():
        if col not in df.columns:
            continue
        if dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(dtype)

    # 6. Ensure canonical schema
    target_cols: List[str] = col_cfg["target"]
    for missing in (set(target_cols) - set(df.columns)):
        df[missing] = np.nan
    df = df[target_cols]  # re-order

    # 7. Remove rows where 'legajo' is null
    if "legajo" in df.columns:
        df = df[df["legajo"].notna()]

    # 8. Normalize date columns
    DATE_COLUMNS = ["FECHA", "FECHA_VIGENCIA", "fecha_inscripcion", "fecha_nacimiento"]
    df = normalize_dates(df, DATE_COLUMNS)

    return df

def write_out(df: pd.DataFrame, cfg: Dict[str, Any]) -> None:
    """Persist the cleaned DataFrame to a CSV file."""
    out = cfg["output"]
    out_path = pathlib.Path(out["path"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        out_path,
        sep=out.get("delimiter", ","),
        encoding=out.get("encoding", "utf-8"),
        index=False,
    )
    logging.info("Saved %s rows to %s", len(df), out_path)

def main() -> None:
    import traceback
    try:
        if len(sys.argv) < 2:
            sys.exit("Usage: python ingest_students.py <config.yml>")

        cfg_path = sys.argv[1]
        cfg = load_config(cfg_path)

        df_raw = read_raw(cfg)
        df_clean = transform(df_raw, cfg)
        write_out(df_clean, cfg)
    except Exception as e:
        logging.error("Exception occurred: %s", e)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()