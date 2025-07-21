#%%
import os
import numpy as np
import pandas as pd
import joblib

from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
#%%
# Conexión a PostgreSQL
PG_URI = os.getenv("PG_URI", "postgresql://user:password@localhost:5432/postgres")
engine = create_engine(PG_URI)

# Leer tabla marts.student_panel
df = pd.read_sql("SELECT * FROM marts.student_panel", engine)
df.head()
#%%
# Eliminar duplicados exactos
df = df.drop_duplicates()

# Convertir columnas numéricas
num_cols = [
    'materias_en_periodo', 'promo_en_periodo', 'nota_media_en_periodo',
    'materias_win3', 'promo_win3', 'nota_win3', 'dias_desde_ult_periodo'
]

df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')

# Verificar target binario
assert df['dropout_next'].isin([0, 1]).all(), "Valores inesperados en dropout_next"
#%%
# Tasa de promoción en el período y ventana 3
df['promo_rate_period'] = df['promo_en_periodo'] / df['materias_en_periodo'].replace(0, np.nan)
df['promo_rate_win3']   = df['promo_win3'] / df['materias_win3'].replace(0, np.nan)

# Materias acumuladas hasta el período actual
df['materias_cum'] = (
    df.sort_values('academic_period')
      .groupby(['legajo', 'cod_carrera'])['materias_en_periodo']
      .cumsum()
)

# Lista de columnas finales
feature_cols_num = num_cols + ['promo_rate_period', 'promo_rate_win3', 'materias_cum']
feature_cols_cat = ['cod_carrera']
#%%
# Variables de entrada y salida
X = df[feature_cols_num + feature_cols_cat]
y = df['dropout_next']

# Split por tiempo (ej. entrenamiento hasta 2022_2C)
train_mask = df['academic_period'] <= '2022_2C'
X_train, y_train = X[train_mask], y[train_mask]
X_val, y_val     = X[~train_mask], y[~train_mask]

print("Entrenamiento:", X_train.shape)
print("Validación:", X_val.shape)
#%%
# Pipelines para numéricas y categóricas
numeric_pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler())
])

categorical_pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("encoder", OneHotEncoder(handle_unknown="ignore"))
])

# Composición
preprocess = ColumnTransformer([
    ("num", numeric_pipe, feature_cols_num),
    ("cat", categorical_pipe, feature_cols_cat)
])

# Clasificador base
clf = GradientBoostingClassifier(random_state=42)

# Pipeline completo
pipeline = Pipeline([
    ("prep", preprocess),
    ("model", clf)
])
#%%
pipeline.fit(X_train, y_train)
print("✅ Entrenamiento completo")
#%%
# Predicciones
preds = pipeline.predict(X_val)
proba = pipeline.predict_proba(X_val)[:, 1]

# Métricas
print("🔎 Classification report:")
print(classification_report(y_val, preds, digits=3))

roc = roc_auc_score(y_val, proba)
print(f"🔁 ROC-AUC: {roc:.3f}")
#%%
