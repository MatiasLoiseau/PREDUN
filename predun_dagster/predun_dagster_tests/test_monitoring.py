import os
import yaml
import time
import pandas as pd
import pytest
import mlflow
from mlflow.tracking import MlflowClient

# Create a simple test of the MLFlow Monitoring Resource
def test_mlflow_monitoring_resource():
    """Simple test to verify MLFlow monitoring resource functionality"""
    from predun_dagster.monitoring import MLFlowMonitoringResource
    
    # Create the monitoring resource with a file-based tracking URI
    monitoring = MLFlowMonitoringResource(
        mlflow_tracking_uri="file:./mlruns",
        experiment_name="test_monitoring"
    )
    
    # Test basic tracking
    with monitoring.track_job("test_job", {"test_tag": "test_value"}):
        # Log some test metrics
        mlflow.log_metric("test_metric", 42)
        
        # Create a test dataframe
        df = pd.DataFrame({
            "col1": [1, 2, 3, None, 5],
            "col2": ["a", "b", "c", "d", None]
        })
        
        # Test data metrics
        monitoring.log_data_metrics("test_table", df)
        
        # Test process metrics
        start_time = time.time() - 2  # Simulate 2 seconds of processing
        monitoring.log_process_metrics(
            process_name="test_process",
            start_time=start_time,
            rows_processed=100,
            success=True
        )
    
    # Verify the experiment exists
    client = MlflowClient(tracking_uri="file:./mlruns")
    experiment = client.get_experiment_by_name("test_monitoring")
    assert experiment is not None, "Experiment was not created"
    
    print("MLFlow monitoring resource test passed successfully!")
    
if __name__ == "__main__":
    test_mlflow_monitoring_resource()
