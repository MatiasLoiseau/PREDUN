import pandas as pd

# Specify the input and output file paths
#input_file = 'data-private/listado_alumnos.xlsx'
#output_file = 'data-private/listado_alumnos.csv'
input_file = 'data-private/CENSALES.xlsx'
output_file = 'data-private/CENSALES.csv'

# Read the Excel file
try:
    df = pd.read_excel(input_file)
    # Save the DataFrame to a CSV file
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"File saved successfully to {output_file}")
except FileNotFoundError:
    print(f"Error: File {input_file} not found.")
except Exception as e:
    print(f"An error occurred: {e}")
