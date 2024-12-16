import pandas as pd

# Cargar el dataset
file_path = "data-private/listado_alumnos_02.csv"
df = pd.read_csv(file_path)

# Requisitos de validación

# 1. 'regular' solo debe ser 'N' o 'S'
valid_regular = ['N', 'S']
df = df[df['regular'].isin(valid_regular)]

# 2. 'anio_academico' solo debe contener años de 4 dígitos
df = df[df['anio_academico'].astype(str).str.fullmatch(r'\d{4}')]

# 3. 'nombre_pertenece' no debe contener valores específicos
invalid_nombre_pertenece = ['D SA', 'D CA', 'D CS', 'D PT', 'UA AC']
df = df[~df['nombre_pertenece'].isin(invalid_nombre_pertenece)]

# 4. 'fecha_inscripcion' debe estar en el formato 'YYYY-MM-DD HH:MM:SS'
df = df[df['fecha_inscripcion'].str.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')]

# Guardar el dataset corregido
output_path = "data-private/listado_alumnos_final.csv"
df.to_csv(output_path, index=False)

print(f"El archivo limpio se ha guardado en {output_path}")
