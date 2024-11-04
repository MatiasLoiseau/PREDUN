import csv
import unicodedata
import os

# Function to replace '{' with '|'
def replace_braces_with_pipe(text):
    return text.replace('{', '|')

# Function to remove Latin special characters
def remove_special_characters(text):
    # Normalize the text to separate accents and special characters
    normalized = unicodedata.normalize('NFKD', text)
    return ''.join([c for c in normalized if not unicodedata.combining(c)])

# Function to process the input text file
def process_file(input_file, output_file):
    with open(input_file, 'r', encoding='ISO-8859-1') as file:
        rows = []
        for line in file:
            # Replace '{' with '|'
            line = replace_braces_with_pipe(line)
            # Remove special characters like ñ, á, etc.
            clean_line = remove_special_characters(line.strip())
            # Split by the | delimiter
            fields = clean_line.split('|')

            # First field is the ID, the rest come in sets of 12 fields
            record_id = fields[0]
            num_fields = 12  # Each record has 12 fields
            for i in range(1, len(fields), num_fields):
                record = [record_id] + fields[i:i + num_fields]
                rows.append(record)

    # Define the specific headers
    headers = [
        "ID", "COD_CARRERA", "NOM_CARRERA", "ANIO", "CUATRIMESTRE",
        "COD_MATERIA", "NOM_MATERIA", "COD_NOSE", "COD_NOSE_2",
        "NOTA", "FECHA_1", "FECHA_2", "RESULTADO"
    ]

    # Write the processed data to a CSV file
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)  # Write the header row
        writer.writerows(rows)     # Write the processed rows

# Get the path to the script's directory
script_dir = os.getcwd()

# Define paths relative to the script's location
input_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA.txt')
output_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA.csv')

# Process the file
process_file(input_file, output_file)

print("Finished")