import pandas as pd
import os

# Get the path to the script's directory
script_dir = os.getcwd()

# Define paths relative to the script's location
input_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_04.csv')
output_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_05.csv')

# Cargar el archivo CSV
df = pd.read_csv(input_file)

# Diccionario de mapeo para los valores de TIPO_CURSADA
mapping = {
    '1#uatrimestre': '1C',
    '2#uatrimestre': '2C',
    '1\u0003uatrimestre': '1C',
    'Cursada de verano': 'V',
    'Anual': 'A',
    '34rimestre': '3T',
    '14rimestre': '1T',
    '24rimestre': '2T',
    'Curso de Verano': 'V',
    'Microcredito-EAD': 'MC',
    'Cursada de Verano': 'V',
    'Verano 2023': 'V',
    '1-ensual': 'M'
}

# Aplicar el mapeo en la columna TIPO_CURSADA
df['TIPO_CURSADA'] = df['TIPO_CURSADA'].map(mapping).fillna(df['TIPO_CURSADA'])

# Convertir columnas de float a int
for col in ['ID', 'ANIO', 'NRO_ACTA', 'RESULTADO']:
    df[col] = df[col].fillna(0).astype(int)

# Guardar el archivo resultante
df.to_csv(output_file, index=False)

print("Archivo guardado exitosamente en:", output_file)
