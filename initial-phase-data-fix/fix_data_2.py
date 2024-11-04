import pandas as pd
import os

# Función para leer el archivo CSV y eliminar filas donde el campo ID no es numérico
def clean_csv(file_path, output_file_path):
    # Leer el archivo CSV
    df = pd.read_csv(file_path)

    # Convertir la columna 'ID' a numérica, forzando errores a NaN (valores no numéricos)
    df['ID'] = pd.to_numeric(df['ID'], errors='coerce')

    # Eliminar filas donde 'ID' es NaN (no numérico)
    df_cleaned = df.dropna(subset=['ID'])

    # Guardar el archivo limpio en un nuevo CSV
    df_cleaned.to_csv(output_file_path, index=False)

    return df_cleaned

# Get the path to the script's directory
script_dir = os.getcwd()

# Define paths relative to the script's location
input_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA.csv')
output_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_02.csv')

# Simulación de uso de la función con el archivo de entrada y salida definidos
cleaned_data = clean_csv(input_file, output_file)
