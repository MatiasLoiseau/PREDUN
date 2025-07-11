import pandas as pd

input_path = "data-private/2025_1C/raw/porcentaje_avance_20250512.txt"
output_path = "data-private/2025_1C/raw/porcentaje_avance_20250512.csv"

df = pd.read_csv(input_path, sep="|", engine="python")

if 'Unnamed: 18' in df.columns:
    df = df.drop(columns=['Unnamed: 18'])

df.to_csv(output_path, index=False)

print(f"File saved as: {output_path}")