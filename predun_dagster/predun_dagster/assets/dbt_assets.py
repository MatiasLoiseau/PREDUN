from dagster_dbt import dbt_assets
from dagster import AssetKey, AssetExecutionContext
from ..constants import DBT_PROJECT_DIR
import json
import os
import time
import mlflow

MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

@dbt_assets(
    manifest=str(MANIFEST_PATH),
    select="canonical.* marts.student_status marts.student_panel",
    required_resource_keys={"dbt", "mlflow_monitoring"}
)
def dbt_project_assets(context: AssetExecutionContext):
    # Start MLFlow tracking for DBT run
    with context.resources.mlflow_monitoring.track_job(
        job_name="dbt_refresh_canonical",
        tags={"dbt_project": str(DBT_PROJECT_DIR), "select": "canonical.* marts.student_status marts.student_panel"}
    ):
        start_time = time.time()
        
        # Track execution of the DBT run
        for event in context.resources.dbt.cli([
            "run", "--select", "canonical.* marts.student_status marts.student_panel"
        ], context=context).stream():
            yield event
            
        # After DBT run completes, collect and log metrics about the models
        try:
            # Log DBT run metrics from run_results.json if it exists
            run_results_path = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")
            if os.path.exists(run_results_path):
                with open(run_results_path, 'r') as f:
                    run_results = json.load(f)
                
                # Calculate overall execution metrics
                execution_time = time.time() - start_time
                mlflow.log_metric("dbt_execution_time_seconds", execution_time)
                
                # Parse run results for additional metrics
                total_models = len(run_results.get('results', []))
                successful_models = sum(1 for r in run_results.get('results', []) if r.get('status') == 'success')
                failed_models = total_models - successful_models
                
                mlflow.log_metric("dbt_total_models", total_models)
                mlflow.log_metric("dbt_successful_models", successful_models)
                mlflow.log_metric("dbt_failed_models", failed_models)
                
                # Log individual model metrics
                for result in run_results.get('results', []):
                    model_name = result.get('unique_id', '').split('.')[-1]
                    if model_name:
                        execution_time = result.get('execution_time', 0)
                        status_success = 1 if result.get('status') == 'success' else 0
                        
                        mlflow.log_metric(f"dbt_model_{model_name}_execution_time", execution_time)
                        mlflow.log_metric(f"dbt_model_{model_name}_success", status_success)
                
                # Log run_results.json as an artifact
                mlflow.log_artifact(run_results_path, "dbt_artifacts")
                
            # Also log manifest.json as an artifact if it exists
            if os.path.exists(str(MANIFEST_PATH)):
                mlflow.log_artifact(str(MANIFEST_PATH), "dbt_artifacts")
                
        except Exception as e:
            context.log.error(f"Error logging DBT metrics: {str(e)}")