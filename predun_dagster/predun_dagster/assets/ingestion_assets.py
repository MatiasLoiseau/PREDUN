
from dagster import asset, AssetExecutionContext, Config
import subprocess



from dagster import asset, AssetExecutionContext
import subprocess

import os

def run_script_with_log(context, cmd):
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        context.log.info(result.stdout)
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running {' '.join(cmd)}: {e.stderr}")
        raise

@asset(config_schema={"version": str})
def format_history_data(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    script = os.path.join(project_root, "ingestion", "01_format_history_data_pre_ingestion.py")
    config = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"v{version}.yaml")
    context.log.info(f"Running {script} with config {config}")
    run_script_with_log(context, [
        "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, config
    ])

@asset(config_schema={"version": str})
def format_students(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    script = os.path.join(project_root, "ingestion", "02_format_students_pre_ingestion.py")
    config = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"students_v{version}.yaml")
    context.log.info(f"Running {script} with config {config}")
    run_script_with_log(context, [
        "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, config
    ])

@asset(config_schema={"version": str})
def format_percentage(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    script = os.path.join(project_root, "ingestion", "03_format_percentage_pre_ingestion.py")
    config = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"percentage_v{version}.yaml")
    context.log.info(f"Running {script} with config {config}")
    run_script_with_log(context, [
        "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, config
    ])

@asset(
    config_schema={
        "version": str,
        "pg_user": str,
        "pg_password": str
    },
    deps=["format_history_data", "format_students", "format_percentage"]
)
def ingest_to_staging(context: AssetExecutionContext):
    version = context.op_config["version"]
    pg_user = context.op_config["pg_user"]
    pg_password = context.op_config["pg_password"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    script = os.path.join(project_root, "ingestion", "04_ingest_to_staging.py")
    pg_conn = f"postgresql://{pg_user}:{pg_password}@localhost:5432/postgres"
    context.log.info(f"Running {script} for version {version} with user {pg_user}")
    run_script_with_log(context, [
        "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script,
        "--period", version, "--root", "data-private", "--pg", pg_conn
    ])
 