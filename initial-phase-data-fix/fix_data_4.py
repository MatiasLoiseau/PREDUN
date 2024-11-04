import pandas as pd
import re

# Ruta del archivo
file_path = '../data-private/CURSADA_HISTORICA_03.csv'

# Cargar el archivo CSV
df = pd.read_csv(file_path)

# Renombrar columnas
column_mapping = {
    'ID': 'ID',
    'COD_CARRERA': 'COD_CARRERA',
    'NOM_CARRERA': 'NOM_CARRERA',
    'ANIO': 'ANIO',
    'CUATRIMESTRE': 'CUATRIMESTRE',
    'COD_MATERIA': 'COD_MATERIA',
    'NOM_MATERIA': 'NOM_MATERIA',
    'COD_NOSE': 'NRO_ACTA',
    'COD_NOSE_2': 'ORIGEN',
    'NOTA': 'NOTA',
    'FECHA_1': 'FECHA',
    'FECHA_2': 'FECHA_VIGENCIA',
    'RESULTADO': 'RESULTADO'
}
df.rename(columns=column_mapping, inplace=True)

# Eliminar el carácter '' de todos los campos de todas las filas
df.replace(to_replace=r'', value='', regex=True, inplace=True)

# Diccionario de mapeo de valores
resultado_mapping = {
    'regular': 1,
    'promociono': 2,
    'abandono': 3,
    'libre': 4,
    'insuficiente': 5,
    'nopromociono': 6
}

# Expresiones regulares para detectar variaciones comunes de los valores en "RESULTADO"
regex_patterns = {
    'regular': r'regular',
    'promociono': r'promocion(o|oull|a)?',
    'abandono': r'abandon(o|oull|a)?',
    'libre': r'libre',
    'insuficiente': r'insuficiente',
    'nopromociono': r'no\s*promocion(o|oull|a)?'
}

# Función para limpiar y mapear valores en la columna 'RESULTADO'
def clean_and_map_resultado(value):
    # Convertir a string y eliminar números y caracteres especiales
    cleaned_value = re.sub(r'[^a-zA-Z]', '', str(value)).strip().lower()

    # Buscar coincidencia en las expresiones regulares
    for key, pattern in regex_patterns.items():
        if re.fullmatch(pattern, cleaned_value):
            return resultado_mapping[key]

    # Si no coincide, mantener el valor original
    return cleaned_value

# Aplicar la función de limpieza y mapeo en la columna 'RESULTADO'
df['RESULTADO'] = df['RESULTADO'].apply(clean_and_map_resultado)

# Eliminar filas con NaN en la columna 'RESULTADO'
df.dropna(subset=['RESULTADO'], inplace=True)

# Guardar el DataFrame modificado en un nuevo archivo CSV
df.to_csv('../data-private/CURSADA_HISTORICA_04.csv', index=False)
