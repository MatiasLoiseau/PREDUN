#!/usr/bin/env python3
"""
Data Ingestion Flexible with Metadata-driven Mapping
Use:
    python ingestion/01_format_history_data_pre_ingestion.py ingestion/mappings/v2024_2C.yml
"""
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

# ---------------------------------------------------------------------
def load_config(cfg_path: str) -> dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize_dates(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            original = df[col].copy()
            parsed = pd.to_datetime(original, format="%d/%m/%Y", errors="coerce")
            mask = parsed.isna()
            parsed[mask] = pd.to_datetime(original[mask], format="%Y-%m-%d", errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d")
    return df

def read_raw(cfg: dict) -> pd.DataFrame:
    inp = cfg["input"]
    path = pathlib.Path(inp["path"])

    if not path.exists():
        raise FileNotFoundError(f"File does not exits: {path}")

    if path.suffix.lower() == ".txt":
        with open(path, "r", encoding=inp.get("encoding", "utf-8")) as fh:
            lines = [l.rstrip("\n") for l in fh]

        if inp.get("header_in_file", False):
            header_line = lines.pop(0)

        rows = [
            l.split(inp.get("delimiter", "|"))
            for l in lines
            if inp.get("expected_columns") is None
            or len(l.split(inp.get("delimiter", "|"))) == inp["expected_columns"]
        ]
        df = pd.DataFrame(rows, columns=cfg["columns"]["source"])

    else:
        df = pd.read_csv(
            path,
            sep=inp.get("delimiter", ","),
            encoding=inp.get("encoding", "utf-8"),
            header=0 if inp.get("header_in_file") else None,
            names=cfg["columns"]["source"] if not inp.get("header_in_file") else None,
        )

    logging.info("%s rows read (sin cabecera) from %s", len(df), path)
    return df

def transform(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    cols_cfg = cfg["columns"]

    # Rename columns   
    df = df.rename(columns=cols_cfg.get("rename", {}))

    # Drop unnecessary columns
    drop_cols = cols_cfg.get("drop", [])
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Map values
    for col, mapping in cfg.get("value_mappings", {}).items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(df[col])

    # Cast types
    NULL_LIKE = cfg.get("null_values", ["NULL", "null", ""])
    df.replace(NULL_LIKE, np.nan, inplace=True)
    for col, dtype in cfg.get("types", {}).items():
        if col in df.columns:
            if dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)

    # Date format to YYYY-MM-DD
    DATE_COLUMNS = ["FECHA", "FECHA_VIGENCIA", "fecha_inscripcion", "fecha_nacimiento"]
    df = normalize_dates(df, DATE_COLUMNS)
    # Remove rows where ID is null
    if "ID" in df.columns:
        df = df[df["ID"].notna()]

    return df

def write_out(df: pd.DataFrame, cfg: dict) -> None:
    out = cfg["output"]
    pathlib.Path(out["path"]).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        out["path"],
        sep=out.get("delimiter", ","),
        encoding=out.get("encoding", "utf-8"),
        index=False,
    )
    logging.info("Final file save in %s (%s filas)", out["path"], len(df))

def main():
    if len(sys.argv) < 2:
        print("Use: python 01_format_history_data_pre_ingestion.py <config.yml>")
        sys.exit(1)

    cfg_path = sys.argv[1]
    cfg = load_config(cfg_path)

    df = read_raw(cfg)
    df = transform(df, cfg)
    write_out(df, cfg)

if __name__ == "__main__":
    main()
