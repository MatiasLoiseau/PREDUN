import os
import time
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from dagster import asset, AssetExecutionContext
import subprocess
import logging


def run_model_in_conda_env(context: AssetExecutionContext):
    """Run model training in the eda-predun conda environment"""
    # Get environment variables for database connection
    pg_uri = os.getenv("PG_URI", "postgresql://user:password@localhost:5432/postgres")
    
    # Script content to run the model
    model_script = f'''
import os
import time
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score

# Setup MLflow
mlflow.set_tracking_uri("http://localhost:8002")
mlflow.set_experiment("student_dropout_prediction")

# Database connection
PG_URI = "{pg_uri}"
engine = create_engine(PG_URI)

# Read data from PostgreSQL
df = pd.read_sql("SELECT * FROM marts.student_panel", engine)

# Data preprocessing - same as sample_model.py
df = df.drop_duplicates()

# Convert numeric columns
num_cols = [
    'materias_en_periodo', 'promo_en_periodo', 'nota_media_en_periodo',
    'materias_win3', 'promo_win3', 'nota_win3', 'dias_desde_ult_periodo'
]

df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')

# Verify target is binary
assert df['dropout_next'].isin([0, 1]).all(), "Unexpected values in dropout_next"

# Feature engineering
df['promo_rate_period'] = df['promo_en_periodo'] / df['materias_en_periodo'].replace(0, np.nan)
df['promo_rate_win3'] = df['promo_win3'] / df['materias_win3'].replace(0, np.nan)

# Cumulative subjects
df['materias_cum'] = (
    df.sort_values('academic_period')
      .groupby(['legajo', 'cod_carrera'])['materias_en_periodo']
      .cumsum()
)

# Feature columns
feature_cols_num = num_cols + ['promo_rate_period', 'promo_rate_win3', 'materias_cum']
feature_cols_cat = ['cod_carrera']

# Variables
X = df[feature_cols_num + feature_cols_cat]
y = df['dropout_next']

# Time-based split (training until 2022_2C)
train_mask = df['academic_period'] <= '2022_2C'
X_train, y_train = X[train_mask], y[train_mask]
X_val, y_val = X[~train_mask], y[~train_mask]

print(f"Training: {{X_train.shape}}")
print(f"Validation: {{X_val.shape}}")

# Start MLflow run
with mlflow.start_run(run_name="student_dropout_model_dagster_training") as run:
    
    # Log parameters
    mlflow.log_param("train_size", X_train.shape[0])
    mlflow.log_param("val_size", X_val.shape[0])
    mlflow.log_param("num_features", len(feature_cols_num))
    mlflow.log_param("cat_features", len(feature_cols_cat))
    mlflow.log_param("train_period_cutoff", "2022_2C")
    
    # Preprocessing pipelines
    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown="ignore"))
    ])

    # Composition
    preprocess = ColumnTransformer([
        ("num", numeric_pipe, feature_cols_num),
        ("cat", categorical_pipe, feature_cols_cat)
    ])

    # Classifier
    clf = GradientBoostingClassifier(random_state=42)

    # Complete pipeline
    pipeline = Pipeline([
        ("prep", preprocess),
        ("model", clf)
    ])
    
    # Log model parameters
    mlflow.log_param("imputer_strategy_num", "median")
    mlflow.log_param("imputer_strategy_cat", "most_frequent")
    mlflow.log_param("scaler", "StandardScaler")
    mlflow.log_param("encoder", "OneHotEncoder")
    mlflow.log_param("classifier", "GradientBoostingClassifier")
    mlflow.log_param("random_state", 42)
    
    # Training
    start_time = time.time()
    pipeline.fit(X_train, y_train)
    training_time = time.time() - start_time
    
    mlflow.log_metric("training_time_seconds", training_time)
    
    # Predictions
    preds = pipeline.predict(X_val)
    proba = pipeline.predict_proba(X_val)[:, 1]
    
    # Metrics
    roc_auc = roc_auc_score(y_val, proba)
    
    mlflow.log_metric("roc_auc", roc_auc)
    mlflow.log_metric("validation_accuracy", (preds == y_val).mean())
    mlflow.log_metric("positive_class_rate", y_val.mean())
    mlflow.log_metric("predicted_positive_rate", preds.mean())
    
    # Log classification report as text
    class_report = classification_report(y_val, preds, digits=3)
    print("Classification report:")
    print(class_report)
    
    # Save classification report as artifact
    with open("classification_report.txt", "w") as f:
        f.write(class_report)
    mlflow.log_artifact("classification_report.txt")
    
    # Log model
    mlflow.sklearn.log_model(
        sk_model=pipeline,
        artifact_path="model",
        registered_model_name="student_dropout_model"
    )
    
    print(f"ROC-AUC: {{roc_auc:.3f}}")
    print(f"Training completed in {{training_time:.2f}} seconds")
    print(f"MLflow run ID: {{run.info.run_id}}")
    
    # Return metrics for Dagster
    import json
    result = {{
        "roc_auc": roc_auc,
        "training_time": training_time,
        "train_size": X_train.shape[0],
        "val_size": X_val.shape[0],
        "run_id": run.info.run_id
    }}
    
    print("DAGSTER_RESULT:" + json.dumps(result))
'''

    # Write script to temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(model_script)
        script_path = f.name
    
    try:
        # Run the script in the eda-predun conda environment
        context.log.info("Starting model training in eda-predun conda environment")
        start_time = time.time()
        
        cmd = [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "eda-predun", 
            "python", script_path
        ]
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        duration = time.time() - start_time
        
        context.log.info(f"Model training completed in {duration:.2f} seconds")
        context.log.info("Training output:")
        context.log.info(result.stdout)
        
        # Extract result from output
        import json
        lines = result.stdout.strip().split('\n')
        for line in lines:
            if line.startswith("DAGSTER_RESULT:"):
                result_data = json.loads(line.replace("DAGSTER_RESULT:", ""))
                context.log.info(f"Training metrics: {result_data}")
                return result_data
        
        # If no result found, return basic info
        return {"status": "completed", "duration": duration}
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running model training: {e.stderr}")
        raise
    finally:
        # Clean up temporary file
        if os.path.exists(script_path):
            os.unlink(script_path)


@asset(
    required_resource_keys={"mlflow_monitoring"},
    deps=["dbt_project_assets"]  # Depends on DBT assets to ensure marts.student_panel exists
)
def train_student_dropout_model(context: AssetExecutionContext):
    """
    Train a student dropout prediction model using data from marts.student_panel.
    
    This asset:
    - Runs in the 'eda-predun' conda environment
    - Reads data from PostgreSQL marts.student_panel table
    - Trains a GradientBoostingClassifier with preprocessing pipeline
    - Logs metrics and model to MLflow running on port 8002
    - Does not save results to CSV files
    
    Returns training metrics and MLflow run information.
    """
    
    # Track the job with MLFlow monitoring
    with context.resources.mlflow_monitoring.track_job(
        job_name="train_student_dropout_model",
        tags={"model_type": "GradientBoostingClassifier", "environment": "eda-predun"}
    ):
        context.log.info("Starting student dropout model training")
        
        # Run the model training in conda environment
        result = run_model_in_conda_env(context)
        
        # Log additional metrics to the MLFlow monitoring resource
        if "roc_auc" in result:
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name="model_training",
                start_time=time.time() - result.get("training_time", 0),
                success=True,
                additional_metrics={
                    "model_roc_auc": result["roc_auc"],
                    "training_samples": result.get("train_size", 0),
                    "validation_samples": result.get("val_size", 0)
                }
            )
        
        context.log.info("Student dropout model training completed successfully")
        return result


@asset
def score_student_dropout_risk(context: AssetExecutionContext):
    """
    Placeholder asset for scoring student dropout risk.
    This could be implemented to score current students using the trained model.
    """
    context.log.info("Scoring asset placeholder - to be implemented")
    return {"status": "placeholder"}