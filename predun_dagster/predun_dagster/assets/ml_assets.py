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


def run_prediction_in_conda_env(context: AssetExecutionContext):
    """Run prediction in the eda-predun conda environment"""
    # Get environment variables for database connection
    pg_uri = os.getenv("PG_URI", "postgresql://user:password@localhost:5432/postgres")
    
    # Script content to run the prediction
    prediction_script = f'''
import os
import time
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine, text
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier

# Setup MLflow
mlflow.set_tracking_uri("http://localhost:8002")
mlflow.set_experiment("student_dropout_prediction")

# Database connection
PG_URI = "{pg_uri}"
engine = create_engine(PG_URI)

print("Loading latest model from MLflow...")

# Get the latest model version from MLflow
client = mlflow.tracking.MlflowClient()
try:
    latest_version = client.get_latest_versions("student_dropout_model", stages=["None"])
    if not latest_version:
        latest_version = client.get_latest_versions("student_dropout_model")
    
    if latest_version:
        model_version = latest_version[0].version
        model_uri = f"models:/student_dropout_model/{{model_version}}"
        print(f"Loading model version {{model_version}}")
        
        # Load the model
        model = mlflow.sklearn.load_model(model_uri)
        print("Model loaded successfully")
    else:
        raise Exception("No trained model found in MLflow")
        
except Exception as e:
    print(f"Error loading model: {{e}}")
    raise

# Get active students from student_status
print("Loading active students data...")
query_students = """
    SELECT DISTINCT legajo, 'estudiando' as status 
    FROM marts.student_status 
    WHERE status = 'estudiando'
"""
active_students = pd.read_sql(query_students, engine)
print(f"Found {{len(active_students)}} active students")

if len(active_students) == 0:
    print("No active students found - exiting")
    exit(0)

# Get student panel data for feature engineering - only for active students
print("Loading student panel data for active students...")
legajos_list = "', '".join(active_students['legajo'].astype(str))
query_panel = f"""
    SELECT * FROM marts.student_panel 
    WHERE legajo IN ('{{legajos_list}}')
    ORDER BY legajo, academic_period DESC
"""
student_panel = pd.read_sql(query_panel, engine)

if len(student_panel) == 0:
    print("No student panel data found for active students - exiting")
    exit(0)

print(f"Loaded {{len(student_panel)}} records from student panel")

# Get the most recent record for each student for prediction
latest_records = student_panel.groupby('legajo').first().reset_index()
print(f"Using {{len(latest_records)}} latest records for prediction")

# Apply the same feature engineering as in training
df = latest_records.copy()

# Convert numeric columns (same as training)
num_cols = [
    'materias_en_periodo', 'promo_en_periodo', 'nota_media_en_periodo',
    'materias_win3', 'promo_win3', 'nota_win3', 'dias_desde_ult_periodo'
]

df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')

# Feature engineering (same as training)
df['promo_rate_period'] = df['promo_en_periodo'] / df['materias_en_periodo'].replace(0, np.nan)
df['promo_rate_win3'] = df['promo_win3'] / df['materias_win3'].replace(0, np.nan)

# For cumulative subjects, use the value as-is since we're taking the latest record
df['materias_cum'] = df['materias_en_periodo'].cumsum()

# Feature columns (same as training)
feature_cols_num = num_cols + ['promo_rate_period', 'promo_rate_win3', 'materias_cum']
feature_cols_cat = ['cod_carrera']

# Prepare features for prediction
X = df[feature_cols_num + feature_cols_cat]

print("Making predictions...")
# Get predictions and probabilities
predictions = model.predict(X)
probabilities = model.predict_proba(X)[:, 1]  # Probability of dropout

print(f"Generated predictions for {{len(predictions)}} students")
print(f"Predicted dropout rate: {{predictions.mean():.3f}}")
print(f"Average dropout probability: {{probabilities.mean():.3f}}")

# Prepare results dataframe
results_df = pd.DataFrame({{
    'legajo': df['legajo'],
    'academic_period': df['academic_period'],
    'cod_carrera': df['cod_carrera'],
    'dropout_prediction': predictions,
    'dropout_probability': probabilities,
    'prediction_date': pd.Timestamp.now(),
    'model_version': model_version
}})

# Create schema if it doesn't exist
with engine.connect() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS predictions"))
    conn.commit()

# Create table if it doesn't exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS predictions.student_dropout_predictions (
    legajo VARCHAR(50),
    academic_period VARCHAR(20),
    cod_carrera VARCHAR(20),
    dropout_prediction INTEGER,
    dropout_probability FLOAT,
    prediction_date TIMESTAMP,
    model_version VARCHAR(50),
    PRIMARY KEY (legajo, prediction_date)
)
"""

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()

print("Saving predictions to database...")

# Save to database
results_df.to_sql(
    'student_dropout_predictions', 
    engine, 
    schema='predictions',
    if_exists='append', 
    index=False,
    method='multi'
)

print(f"Successfully saved {{len(results_df)}} predictions to predictions.student_dropout_predictions")

# Return summary statistics
summary = {{
    "total_students": len(results_df),
    "predicted_dropouts": int(predictions.sum()),
    "dropout_rate": float(predictions.mean()),
    "avg_dropout_probability": float(probabilities.mean()),
    "model_version": model_version,
    "prediction_date": pd.Timestamp.now().isoformat()
}}

import json
print("DAGSTER_RESULT:" + json.dumps(summary))
'''

    # Write script to temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(prediction_script)
        script_path = f.name
    
    try:
        # Run the script in the eda-predun conda environment
        context.log.info("Starting prediction generation in eda-predun conda environment")
        start_time = time.time()
        
        cmd = [
            "/Users/matiasloiseau/anaconda3/bin/conda", "run", "-n", "eda-predun", 
            "python", script_path
        ]
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        duration = time.time() - start_time
        
        context.log.info(f"Prediction generation completed in {duration:.2f} seconds")
        context.log.info("Prediction output:")
        context.log.info(result.stdout)
        
        # Extract result from output
        import json
        lines = result.stdout.strip().split('\n')
        for line in lines:
            if line.startswith("DAGSTER_RESULT:"):
                result_data = json.loads(line.replace("DAGSTER_RESULT:", ""))
                context.log.info(f"Prediction summary: {result_data}")
                return result_data
        
        # If no result found, return basic info
        return {"status": "completed", "duration": duration}
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running prediction: {e.stderr}")
        raise
    finally:
        # Clean up temporary file
        if os.path.exists(script_path):
            os.unlink(script_path)


@asset(
    required_resource_keys={"mlflow_monitoring"},
    deps=["train_student_dropout_model", "dbt_project_assets"]  # Depends on trained model and DBT data
)
def predict_student_dropout_risk(context: AssetExecutionContext):
    """
    Generate dropout risk predictions for active students using the latest trained model.
    
    This asset:
    - Loads the latest model from MLflow
    - Gets active students from marts.student_status where status='estudiando'
    - Applies the same feature engineering as training
    - Makes predictions and saves them to predictions.student_dropout_predictions table
    - Runs in the 'eda-predun' conda environment
    
    Returns prediction summary statistics.
    """
    
    # Track the job with MLFlow monitoring
    with context.resources.mlflow_monitoring.track_job(
        job_name="predict_student_dropout_risk",
        tags={"model_type": "prediction", "environment": "eda-predun"}
    ):
        context.log.info("Starting student dropout risk prediction")
        
        # Run the prediction in conda environment
        result = run_prediction_in_conda_env(context)
        
        # Log additional metrics to the MLFlow monitoring resource
        if "total_students" in result:
            context.resources.mlflow_monitoring.log_process_metrics(
                process_name="model_prediction",
                start_time=time.time() - result.get("duration", 0),
                success=True,
                additional_metrics={
                    "predicted_students": result.get("total_students", 0),
                    "predicted_dropouts": result.get("predicted_dropouts", 0),
                    "dropout_rate": result.get("dropout_rate", 0),
                    "avg_dropout_probability": result.get("avg_dropout_probability", 0)
                }
            )
        
        context.log.info("Student dropout risk prediction completed successfully")
        return result