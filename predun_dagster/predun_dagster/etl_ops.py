import subprocess, shlex
from dagster import op, get_dagster_logger

def _run(cmd: str):
    get_dagster_logger().info(cmd)
    subprocess.run(shlex.split(cmd), check=True)

@op(config_schema={"period": str})
def clean_files(context):
    p = context.op_config["period"]
    _run(f"python ingestion/01_fix_clean_data_pre_ingestion.py "
         f"ingestion/mappings/fix_and_clean/v{p}.yaml")
    _run(f"python ingestion/02_fix_clean_students_pre_ingestion.py "
         f"ingestion/mappings/fix_and_clean/students_{p}.yml")
    _run(f"python ingestion/03_fix_clean_percentage_pre_ingestion.py "
         f"ingestion/mappings/fix_and_clean/percentage_{p}.yml")
    return p

@op
def ingest_to_staging(period: str):
    _run(f"python ingestion/04_ingest_to_staging.py "
         f"--period {period} --root data-private "
         f'--pg "postgresql://siu:siu@localhost:5432/postgres"')
    return period

@op
def refresh_canonical(period: str):
    _run("python predun_dbt/scripts/refresh_canonical.py "
         "--project-dir predun_dbt/")