'''
The script extracts the following features for each student:
- **Number of courses enrolled** (`materias_cursadas`)
- **Number of courses passed** (`materias_aprobadas`)
- **Number of courses abandoned** (`materias_abandonadas`)
- **Total time spent in university** (`tiempo_total_cursada`)
- **Proportion of passed courses** (`proporcion_aprobadas`)

The target variable (`ABANDONO`) is set to `1` if the student dropped out and `0` if they completed their program.

## Machine Learning Model
- The dataset is split into training (80%) and testing (20%) sets.
- An **XGBoost Classifier** is used for prediction.
- Model evaluation is performed using **accuracy** and a **classification report**.
'''

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report

cursada_df = pd.read_csv('data-private/CURSADA_HISTORICA_final.csv')
avance_df = pd.read_csv('data-private/porcentaje_avance_100_por_100.csv')

cursada_df.columns = cursada_df.columns.str.strip()
avance_df.columns = avance_df.columns.str.strip()

cursada_df['FECHA'] = pd.to_datetime(cursada_df['FECHA'], errors='coerce')
cursada_df['FECHA_VIGENCIA'] = pd.to_datetime(cursada_df['FECHA_VIGENCIA'], errors='coerce')

features_df = cursada_df.groupby('ID').agg(
    materias_cursadas=('COD_MATERIA', 'count'),
    materias_aprobadas=('RESULTADO', lambda x: (x == 'Aprobó').sum()),
    materias_abandonadas=('RESULTADO', lambda x: (x == 'Abandonó').sum()),
    tiempo_total_cursada=('FECHA_VIGENCIA', lambda x: (x.max() - x.min()).days if len(x.dropna()) > 0 else np.nan)
).reset_index()

features_df['proporcion_aprobadas'] = features_df['materias_aprobadas'] / features_df['materias_cursadas']
features_df.fillna(0, inplace=True)

completaron = set(avance_df['ID'])
features_df['ABANDONO'] = features_df['ID'].apply(lambda x: 0 if x in completaron else 1)

X = features_df.drop(columns=['ID', 'ABANDONO'])
y = features_df['ABANDONO']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print("Classification Report:\n", classification_report(y_test, y_pred))