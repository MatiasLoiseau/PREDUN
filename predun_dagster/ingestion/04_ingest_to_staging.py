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
    import logging
    period_path = root / period
    resolved_path = period_path.resolve()
    logging.info(f"Resolved period_path: {resolved_path}")
    print(f"[DEBUG] Resolved period_path: {resolved_path}")
    if not period_path.exists():
        logging.error(f"Period path does not exist: {resolved_path}")
        print(f"[ERROR] Period path does not exist: {resolved_path}")
        raise FileNotFoundError(f"Period path does not exist: {resolved_path}")
    csv_files = list(period_path.glob("*.csv"))
    logging.info(f"Found CSV files: {[str(f) for f in csv_files]}")
    print(f"[DEBUG] Found CSV files: {[str(f) for f in csv_files]}")

    with conn.cursor() as cur:
        for csv_path in csv_files:
            tbl = target_table(csv_path.name)
            if not tbl:
                logging.warning(f"Ignored file: {csv_path.name}")
                continue

            logging.info(f"Processing file: {csv_path.name} for table: {tbl}")
            ensure_partition(cur, tbl, period)

            with csv_path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [(period, json.dumps(row)) for row in reader]

            logging.info(f"Inserting {len(rows)} rows into {tbl}")
            if rows:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {tbl} (academic_period, payload) VALUES %s",
                    rows,
                    page_size=10_000
                )
                logging.info(f"{csv_path.name} → {tbl} ({len(rows)} filas)")
            else:
                logging.warning(f"No rows found in {csv_path.name}")

    conn.commit()
    logging.info("Database commit complete.")

if __name__ == "__main__":
    import traceback, logging
    log_file = "ingest_to_staging.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s • %(levelname)s • %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    print(f"[DEBUG] Logging to file: {log_file}")
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("--period", required=True, help="2024_2C, 2025_1C, …")
        ap.add_argument("--root",   default="data-private", type=pathlib.Path)
        ap.add_argument("--pg",     required=True, help="postgresql:// …")
        args = ap.parse_args()

        with psycopg2.connect(args.pg) as conn:
            ingest(args.period, args.root, conn)
    except Exception as e:
        logging.error("Exception occurred: %s", e)
        traceback.print_exc()
        sys.exit(1)