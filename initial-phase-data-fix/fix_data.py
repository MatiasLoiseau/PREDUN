import pandas as pd

# Input and output file paths
input_file = "data-private/CURSADA_HISTORICA.txt"
output_file = "data-private/CURSADA_HISTORICA.csv"

# Define column names
columns = [
    "ID", "COD_CARRERA", "NOM_CARRERA", "ANIO", "TIPO_CURSADA",
    "COD_MATERIA", "NOM_MATERIA", "NRO_ACTA", "ORIGEN", "NOTA",
    "FECHA", "FECHA_VIGENCIA", "RESULTADO"
]

# Read the text file with the appropriate encoding
try:
    with open(input_file, 'r', encoding='ISO-8859-15') as file:
        # Read lines and filter those that have exactly 13 columns
        lines = [line.strip() for line in file if len(line.split('|')) == len(columns)]
except Exception as e:
    print(f"Error reading the file: {e}")
    exit(1)

# Convert filtered lines into a list of lists
data = [line.split('|') for line in lines]

# Create a DataFrame
df = pd.DataFrame(data, columns=columns)

# Export the DataFrame to a CSV file
try:
    df.to_csv(output_file, index=False, sep=',', encoding='utf-8')
    print(f"CSV file successfully generated: {output_file}")
except Exception as e:
    print(f"Error writing the CSV file: {e}")
