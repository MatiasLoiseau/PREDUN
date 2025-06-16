#!/usr/bin/env python3
"""
Dispara la macro refresh_canonical de dbt desde Python.

Requisitos:
    pip install dbt-postgres
Uso:
    python scripts/refresh_canonical.py --project-dir /ruta/a/my_dbt_project
"""

import argparse
import sys
from dbt.cli.main import dbtRunner, dbtRunnerResult

def run_refresh(project_dir: str) -> dbtRunnerResult:
    runner = dbtRunner()
    return runner.invoke([
        "run-operation", "refresh_canonical",
        "--project-dir", project_dir
    ])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True,
                    help="Ruta al directorio raíz del proyecto dbt")
    args = ap.parse_args()

    result = run_refresh(args.project_dir)

    # Imprime información de la ejecución
    print(f"Éxito: {result.success}")
    for r in result.result:
        print(r)  # o bien acceder a r.node, r.status si querés info más detallada

    sys.exit(0 if result.success else 1)