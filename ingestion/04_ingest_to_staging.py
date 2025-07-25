#!/usr/bin/env python3
"""
Add every data from csv to staging tables
Usage
-----
    python ingest_to_staging.py --period 2024_2C --root data-private \
                                --pg "postgresql://user:password@localhost:5432/postgres"
"""

import argparse, csv, json, pathlib, psycopg2, psycopg2.extras, re, sys
from typing import Dict
from typing import Optional

FILE_MAP: Dict[str, str] = {
    # name → staging_name
    r"cursada":        "staging.cursada_historica_raw",
    r"alumno":         "staging.alumnos_raw",
    r"porcentaje":     "staging.porcentaje_avance_raw",
}

def target_table(filename: str) -> Optional[str]:
    for pattern, table in FILE_MAP.items():
        if re.search(pattern, filename, re.I):
            return table
    return None

def ensure_partition(cur, table: str, period: str) -> None:
    schema, base = table.split(".")
    part = f"{schema}.{base}_{period}"
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {part}
        PARTITION OF {table}
        FOR VALUES IN (%s)""", (period,))

def ingest(period: str, root: pathlib.Path, conn):
    period_path = root / period
    csv_files   = period_path.glob("*.csv")

    with conn.cursor() as cur:
        for csv_path in csv_files:
            tbl = target_table(csv_path.name)
            if not tbl:
                print(f" Ignored file: {csv_path.name}", file=sys.stderr)
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

    conn.commit()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", required=True, help="2024_2C, 2025_1C, …")
    ap.add_argument("--root",   default="data-private", type=pathlib.Path)
    ap.add_argument("--pg",     required=True, help="postgresql:// …")
    args = ap.parse_args()

    with psycopg2.connect(args.pg) as conn:
        ingest(args.period, args.root, conn)