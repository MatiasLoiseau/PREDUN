import time
import os
import mlflow
from mlflow.tracking import MlflowClient
from dagster import resource, ConfigurableResource, get_dagster_logger
from contextlib import contextmanager
import pandas as pd
from typing import Dict, Any, List, Optional


class MLFlowMonitoringResource(ConfigurableResource):
    """Resource for monitoring data pipeline metrics using MLFlow."""
    
    mlflow_tracking_uri: str
    experiment_name: str
    
    def setup_mlflow(self):
        """Setup MLFlow tracking with the configured URI."""
        # Use Dagster logger instead of self.log
        logger = get_dagster_logger()
        
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        
        # Check if experiment exists, if not create it
        client = MlflowClient()
        experiment = client.get_experiment_by_name(self.experiment_name)
        
        if experiment is None:
            try:
                experiment_id = mlflow.create_experiment(self.experiment_name)
                logger.info(f"Created new MLFlow experiment: {self.experiment_name} (ID: {experiment_id})")
            except mlflow.exceptions.MlflowException as e:
                # Handle case where experiment was created by another process
                if "already exists" in str(e):
                    experiment = client.get_experiment_by_name(self.experiment_name)
                    logger.info(f"Using existing MLFlow experiment: {self.experiment_name} (ID: {experiment.experiment_id})")
                else:
                    raise
        else:
            logger.info(f"Using existing MLFlow experiment: {self.experiment_name} (ID: {experiment.experiment_id})")
            
        mlflow.set_experiment(self.experiment_name)
    
    @contextmanager
    def track_job(self, job_name: str, tags: Optional[Dict[str, str]] = None):
        """Context manager to track a Dagster job run with MLFlow."""
        # Use Dagster logger instead of self.log
        logger = get_dagster_logger()
        
        self.setup_mlflow()
        
        # Start MLFlow run
        run_tags = tags or {}
        run_tags["job_name"] = job_name
        
        with mlflow.start_run(run_name=f"{job_name}_{int(time.time())}", tags=run_tags) as run:
            logger.info(f"Started MLFlow run: {run.info.run_id} for job {job_name}")
            
            # Record job start time
            start_time = time.time()
            
            try:
                yield run
                
                # Record job end time and duration
                end_time = time.time()
                duration = end_time - start_time
                
                # Log final metrics
                mlflow.log_metric("job_duration_seconds", duration)
                mlflow.log_metric("job_success", 1)
                
                logger.info(f"Successfully completed MLFlow run: {run.info.run_id} in {duration:.2f} seconds")
            
            except Exception as e:
                # Log failure
                mlflow.log_metric("job_success", 0)
                mlflow.set_tag("error_message", str(e))
                
                logger.error(f"Failed MLFlow run: {run.info.run_id} with error: {str(e)}")
                raise
    
    def log_data_metrics(self, table_name: str, df: pd.DataFrame):
        """Log metrics about a dataframe during processing"""
        # Log basic DataFrame metrics
        mlflow.log_metric(f"{table_name}_row_count", len(df))
        mlflow.log_metric(f"{table_name}_column_count", len(df.columns))
        
        # Log memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        mlflow.log_metric(f"{table_name}_memory_usage_bytes", memory_usage)
        
        # Log null counts per column
        null_counts = df.isnull().sum().to_dict()
        for col, count in null_counts.items():
            if count > 0:  # Only log columns with nulls
                mlflow.log_metric(f"{table_name}_{col}_null_count", count)
        
        # Log basic statistics for numeric columns
        for col in df.select_dtypes(include=['number']).columns:
            if not df[col].isnull().all():  # Skip if all values are null
                mlflow.log_metric(f"{table_name}_{col}_mean", df[col].mean())
                mlflow.log_metric(f"{table_name}_{col}_min", df[col].min())
                mlflow.log_metric(f"{table_name}_{col}_max", df[col].max())
    
    def log_process_metrics(self, process_name: str, start_time: float, rows_processed: int = 0, 
                          error_count: int = 0, success: bool = True, 
                          additional_metrics: Optional[Dict[str, float]] = None):
        """Log metrics about a processing step"""
        # Calculate duration
        duration = time.time() - start_time
        
        # Log standard metrics
        mlflow.log_metric(f"{process_name}_duration_seconds", duration)
        mlflow.log_metric(f"{process_name}_success", 1 if success else 0)
        
        if rows_processed > 0:
            mlflow.log_metric(f"{process_name}_rows_processed", rows_processed)
            if duration > 0:
                mlflow.log_metric(f"{process_name}_rows_per_second", rows_processed / duration)
        
        if error_count > 0:
            mlflow.log_metric(f"{process_name}_error_count", error_count)
        
        # Log any additional metrics
        if additional_metrics:
            for name, value in additional_metrics.items():
                mlflow.log_metric(f"{process_name}_{name}", value)
    
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None):
        """Log a file as an artifact"""
        logger = get_dagster_logger()
        mlflow.log_artifact(local_path, artifact_path)
        logger.info(f"Logged artifact: {local_path} to path: {artifact_path or 'root'}")


@resource(config_schema={"mlflow_tracking_uri": str, "experiment_name": str})
def mlflow_monitoring_resource(context):
    """Factory for creating the MLFlowMonitoringResource"""
    return MLFlowMonitoringResource(
        mlflow_tracking_uri=context.resource_config["mlflow_tracking_uri"],
        experiment_name=context.resource_config["experiment_name"],
    )
