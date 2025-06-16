#!/usr/bin/env python3
"""
Ingresa TODOS los CSV de un cuatrimestre en las tablas staging.*_raw
Usage
-----
    python ingest_to_staging.py --period 2024_2C --root data-private \
                                --pg "postgresql://user:password@localhost:5432/postgres"
"""

import argparse, csv, json, pathlib, psycopg2, psycopg2.extras, re, sys
from typing import Dict

# -------- utilidades -------------------------------------------------------
FILE_MAP: Dict[str, str] = {
    # patron_en_el_nombre → tabla_staging
    r"cursada":        "staging.cursada_historica_raw",
    r"alumno":         "staging.alumnos_raw",
    r"porcentaje":     "staging.porcentaje_avance_raw",
}

def target_table(filename: str) -> str | None:
    for pattern, table in FILE_MAP.items():
        if re.search(pattern, filename, re.I):
            return table
    return None

def ensure_partition(cur, table: str, period: str) -> None:
    part = f"{table.split('.')[-1]}_{period}"
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {part}
        PARTITION OF {table}
        FOR VALUES IN (%s)""", (period,))

# -------- proceso ----------------------------------------------------------
def ingest(period: str, root: pathlib.Path, conn):
    period_path = root / period
    csv_files   = period_path.glob("*.csv")

    with conn, conn.cursor() as cur:
        for csv_path in csv_files:
            tbl = target_table(csv_path.name)
            if not tbl:
                print(f" Archivo ignorado: {csv_path.name}", file=sys.stderr)
                continue

            ensure_partition(cur, tbl, period)

            with csv_path.open(encoding="utf-8") as f:
                reader  = csv.DictReader(f)
                rows    = [(period, json.dumps(row)) for row in reader]

            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {tbl} (academic_period, payload) VALUES %s",
                rows,
                page_size=10_000
            )
            print(f" {csv_path.name} → {tbl} ({len(rows)} filas)")

# -------- CLI --------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", required=True, help="2024_2C, 2025_1C, …")
    ap.add_argument("--root",   default="data-private", type=pathlib.Path)
    ap.add_argument("--pg",     required=True, help="postgresql:// …")
    args = ap.parse_args()

    with psycopg2.connect(args.pg) as conn:
        ingest(args.period, args.root, conn)