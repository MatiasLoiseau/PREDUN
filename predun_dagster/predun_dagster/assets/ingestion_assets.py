
from dagster import asset, AssetExecutionContext, Config
import subprocess
import os
import tempfile
import yaml
import logging
import time
import pandas as pd

def update_yaml_config_paths(config_path, workspace_root):
    """Update relative paths in YAML config to use absolute paths"""
    try:
        with open(config_path, 'r') as f:
            cfg = yaml.safe_load(f)
        
        # Update input path if it exists and is relative
        if 'input' in cfg and 'path' in cfg['input']:
            path = cfg['input']['path']
            if path.startswith('../'):
                # Replace relative path with absolute path
                cfg['input']['path'] = os.path.join(workspace_root, path.replace('../', ''))
        
        # Create temporary file with updated config
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.yaml')
        with open(temp_file.name, 'w') as f:
            yaml.dump(cfg, f)
        
        return temp_file.name
    except Exception as e:
        logging.error(f"Error updating YAML config paths: {e}")
        raise

def run_script_with_log(context, cmd):
    try:
        start_time = time.time()
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        duration = time.time() - start_time
        
        context.log.info(result.stdout)
        
        # If MLFlow monitoring is available, log execution metrics
        if hasattr(context.resources, 'mlflow_monitoring'):
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name=cmd[-1].split('/')[-1],
                start_time=start_time,
                success=True,
                additional_metrics={
                    "execution_time": duration,
                    "command_length": len(' '.join(cmd))
                }
            )
        
        return result
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running {' '.join(cmd)}: {e.stderr}")
        
        # If MLFlow monitoring is available, log failure metrics
        if hasattr(context.resources, 'mlflow_monitoring'):
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name=cmd[-1].split('/')[-1],
                start_time=start_time,
                success=False,
                error_count=1,
                additional_metrics={
                    "error_code": e.returncode
                }
            )
        
        raise

@asset(config_schema={"version": str}, required_resource_keys={"mlflow_monitoring"})
def format_history_data(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "01_format_history_data_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"v{version}.yaml")
    
    # Track with MLFlow
    with context.resources.mlflow_monitoring.track_job(
        job_name="format_history_data",
        tags={"version": version, "script": script}
    ):
        # Update config paths
        updated_config = update_yaml_config_paths(config_path, workspace_root)
        
        try:
            context.log.info(f"Running {script} with config {updated_config}")
            result = run_script_with_log(context, [
                "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
            ])
            
            # Try to read and log metrics about the output file if it exists
            output_dir = os.path.join(workspace_root, "data-private", version, "CURSADA_HISTORICA_final.csv")
            try:
                if os.path.exists(output_dir):
                    df = pd.read_csv(output_dir)
                    context.resources.mlflow_monitoring.log_data_metrics("cursada_historica", df)
                    context.log.info(f"Logged metrics for cursada_historica with {len(df)} rows")
            except Exception as e:
                context.log.warning(f"Failed to log data metrics: {str(e)}")
                
        finally:
            # Clean up the temporary file
            if os.path.exists(updated_config):
                os.unlink(updated_config)
                context.log.debug(f"Removed temporary config file: {updated_config}")

@asset(config_schema={"version": str}, required_resource_keys={"mlflow_monitoring"})
def format_students(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "02_format_students_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"students_v{version}.yaml")
    
    # Track with MLFlow
    with context.resources.mlflow_monitoring.track_job(
        job_name="format_students",
        tags={"version": version, "script": script}
    ):
        # Update config paths
        updated_config = update_yaml_config_paths(config_path, workspace_root)
        
        try:
            context.log.info(f"Running {script} with config {updated_config}")
            result = run_script_with_log(context, [
                "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
            ])
            
            # Try to read and log metrics about the output file if it exists
            output_dir = os.path.join(workspace_root, "data-private", version, "listado_alumnos_clean.csv")
            try:
                if os.path.exists(output_dir):
                    df = pd.read_csv(output_dir)
                    context.resources.mlflow_monitoring.log_data_metrics("listado_alumnos", df)
                    context.log.info(f"Logged metrics for listado_alumnos with {len(df)} rows")
            except Exception as e:
                context.log.warning(f"Failed to log data metrics: {str(e)}")
                
        finally:
            # Clean up the temporary file
            if os.path.exists(updated_config):
                os.unlink(updated_config)
                context.log.debug(f"Removed temporary config file: {updated_config}")

@asset(config_schema={"version": str}, required_resource_keys={"mlflow_monitoring"})
def format_percentage(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "03_format_percentage_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"percentage_v{version}.yaml")
    
    # Track with MLFlow
    with context.resources.mlflow_monitoring.track_job(
        job_name="format_percentage",
        tags={"version": version, "script": script}
    ):
        # Update config paths
        updated_config = update_yaml_config_paths(config_path, workspace_root)
        
        try:
            context.log.info(f"Running {script} with config {updated_config}")
            result = run_script_with_log(context, [
                "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
            ])
            
            # Try to read and log metrics about the output file if it exists
            output_dir = os.path.join(workspace_root, "data-private", version, "porcentaje_avance_100_por_100_clean.csv")
            try:
                if os.path.exists(output_dir):
                    df = pd.read_csv(output_dir)
                    context.resources.mlflow_monitoring.log_data_metrics("porcentaje_avance", df)
                    context.log.info(f"Logged metrics for porcentaje_avance with {len(df)} rows")
            except Exception as e:
                context.log.warning(f"Failed to log data metrics: {str(e)}")
                
        finally:
            # Clean up the temporary file
            if os.path.exists(updated_config):
                os.unlink(updated_config)
                context.log.debug(f"Removed temporary config file: {updated_config}")

@asset(
    config_schema={
        "version": str,
        "pg_user": str,
        "pg_password": str
    },
    deps=["format_history_data", "format_students", "format_percentage"],
    required_resource_keys={"mlflow_monitoring"}
)
def ingest_to_staging(context: AssetExecutionContext):
    version = context.op_config["version"]
    pg_user = context.op_config["pg_user"]
    pg_password = context.op_config["pg_password"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "04_ingest_to_staging.py")
    pg_conn = f"postgresql://{pg_user}:{pg_password}@localhost:5432/postgres"
    
    # Use absolute path to data-private
    data_private_path = os.path.join(workspace_root, "data-private")
    
    # Track with MLFlow
    with context.resources.mlflow_monitoring.track_job(
        job_name="ingest_to_staging",
        tags={"version": version, "script": script}
    ):
        context.log.info(f"Running {script} for version {version} with user {pg_user}")
        context.log.info(f"Using data path: {data_private_path}")
        
        start_time = time.time()
        result = run_script_with_log(context, [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script,
            "--period", version, "--root", data_private_path, "--pg", pg_conn
        ])
        
        # Log the SQL script as an artifact if it exists
        sql_script = os.path.join(project_root, "ingestion", "tables_creation.sql")
        if os.path.exists(sql_script):
            context.resources.mlflow_monitoring.log_artifact(sql_script, "sql_scripts")