import pandas as pd

# Load the CSV file
file_path = 'data-private/listado_alumnos.csv'
df = pd.read_csv(file_path)

# Step 1: Drop rows where 'legajo' is null
df = df.dropna(subset=['legajo'])

# Step 2: Drop unwanted columns
columns_to_drop = ['Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15']
df = df.drop(columns=columns_to_drop, errors='ignore')

# Step 3: Rename columns
column_renaming = {
    'codigo': 'codigo_carrera',
    'nombre-2': 'nombre_carrera',
    'codigo-2': 'codigo_pertenece',
    'nombre-3': 'nombre_pertenece'
}
df = df.rename(columns=column_renaming)

# Save the cleaned DataFrame to a new CSV file
output_file = 'data-private/listado_alumnos_02.csv'
df.to_csv(output_file, index=False)

print(f"Cleaned CSV saved to {output_file}")