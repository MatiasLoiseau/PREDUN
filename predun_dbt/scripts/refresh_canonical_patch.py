#!/usr/bin/env python3
"""
dbt run -m canonical_cursada_historica \
        --vars '{"academic_period": "2025_1C"}'

or run this script

python scripts/refresh_canonical_patch.py \
       --project-dir predun_dbt \
       --period 2025_1C
"""
import argparse, sys
from dbt.cli.main import dbtRunner

def run_model(project_dir: str, academic_period: str):
    runner = dbtRunner()
    return runner.invoke([
        "run",
        "--models", "canonical_cursada_historica",
        "--project-dir", project_dir,
        "--vars", f"academic_period: '{academic_period}'"
    ])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--period", required=True,
                    help="Ej: 2025_1C")
    args = ap.parse_args()

    res = run_model(args.project_dir, args.period)
    print(f"Success: {res.success}")
    sys.exit(0 if res.success else 1)