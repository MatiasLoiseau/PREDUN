import pandas as pd

# Define a mapping for TIPO_CURSADA values
tipo_cursada_mapping = {
    '1° cuatrimestre': '1C',
    '2° cuatrimestre': '2C',
    '1º Cuatrimestre': '1C',
    'Cursada de verano': 'V',
    'Anual': 'A',
    '3° trimestre': '3T',
    '1° trimestre': '1T',
    '2° trimestre': '2T',
    'Curso de Verano': 'V',
    'Microcredito-EAD': 'MC',
    'Cursada de Verano': 'V',
    'Verano 2023': 'V',
    '1° mensual': 'M',
    '2° Trimestre': '2T',
    '2° bimestre': '2B'
}

# Load the CSV file
input_file = 'data-private/CURSADA_HISTORICA.csv'
df = pd.read_csv(input_file)

# Apply the mapping to the TIPO_CURSADA column
df['TIPO_CURSADA'] = df['TIPO_CURSADA'].map(tipo_cursada_mapping)

# Save the modified data to a new CSV file
output_file = 'data-private/CURSADA_HISTORICA_02.csv'
df.to_csv(output_file, index=False)

print(f"File saved as {output_file}")
