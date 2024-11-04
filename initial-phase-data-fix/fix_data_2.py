import pandas as pd

# Función para leer el archivo CSV y eliminar filas donde el campo ID no es numérico
def clean_csv(file_path):
    # Leer el archivo CSV
    df = pd.read_csv(file_path)

    # Convertir la columna 'ID' a numérica, forzando errores a NaN (valores no numéricos)
    df['ID'] = pd.to_numeric(df['ID'], errors='coerce')

    # Eliminar filas donde 'ID' es NaN (no numérico)
    df_cleaned = df.dropna(subset=['ID'])

    # Guardar el archivo limpio en un nuevo CSV
    df_cleaned.to_csv('../data-private/CURSADA_HISTORICA_02.csv', index=False)

    return df_cleaned

# Simulación de uso de la función con un archivo de ejemplo
cleaned_data = clean_csv('../data-private/CURSADA_HISTORICA.csv')
