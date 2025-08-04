
from dagster import asset, AssetExecutionContext, Config
import subprocess
import os
import tempfile
import yaml
import logging

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
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        context.log.info(result.stdout)
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running {' '.join(cmd)}: {e.stderr}")
        raise

@asset(config_schema={"version": str})
def format_history_data(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "01_format_history_data_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"v{version}.yaml")
    
    # Update config paths
    updated_config = update_yaml_config_paths(config_path, workspace_root)
    
    try:
        context.log.info(f"Running {script} with config {updated_config}")
        run_script_with_log(context, [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
        ])
    finally:
        # Clean up the temporary file
        if os.path.exists(updated_config):
            os.unlink(updated_config)
            context.log.debug(f"Removed temporary config file: {updated_config}")

@asset(config_schema={"version": str})
def format_students(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "02_format_students_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"students_v{version}.yaml")
    
    # Update config paths
    updated_config = update_yaml_config_paths(config_path, workspace_root)
    
    try:
        context.log.info(f"Running {script} with config {updated_config}")
        run_script_with_log(context, [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
        ])
    finally:
        # Clean up the temporary file
        if os.path.exists(updated_config):
            os.unlink(updated_config)
            context.log.debug(f"Removed temporary config file: {updated_config}")

@asset(config_schema={"version": str})
def format_percentage(context: AssetExecutionContext):
    version = context.op_config["version"]
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    workspace_root = os.path.dirname(project_root)
    script = os.path.join(project_root, "ingestion", "03_format_percentage_pre_ingestion.py")
    config_path = os.path.join(project_root, "ingestion", "mappings", "fix_and_clean", f"percentage_v{version}.yaml")
    
    # Update config paths
    updated_config = update_yaml_config_paths(config_path, workspace_root)
    
    try:
        context.log.info(f"Running {script} with config {updated_config}")
        run_script_with_log(context, [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script, updated_config
        ])
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
    deps=["format_history_data", "format_students", "format_percentage"]
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
    
    context.log.info(f"Running {script} for version {version} with user {pg_user}")
    context.log.info(f"Using data path: {data_private_path}")
    
    run_script_with_log(context, [
        "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "dagster-predun", "python", script,
        "--period", version, "--root", data_private_path, "--pg", pg_conn
    ])
 