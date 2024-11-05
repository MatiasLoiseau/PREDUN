import csv
from datetime import datetime
import os

# Validation functions
def validate_ID(value):
    if value == '':
        return True
    try:
        f = float(value)
        return f.is_integer()
    except ValueError:
        return False

def validate_COD_CARRERA(value):
    if value == '':
        return True
    return len(value) < 6

def validate_ANIO(value):
    if value == '':
        return True
    try:
        f = float(value)
        return f.is_integer() and 1000 <= int(f) <= 9999
    except ValueError:
        return False

def validate_CUATRIMESTRE(value):
    if value == '':
        return True
    allowed_values = [
        "1#uatrimestre", "2#uatrimestre", "Cursada de verano", "anual",
        "Curso de Verano", "1uatrimestre", "Anual", "24rimestre",
        "14rimestre", "34rimestre", "1-ensual", "Cursada de Verano",
        "Verano 2023", "Microcredito-EAD"
    ]
    return value in allowed_values

def validate_COD_MATERIA(value):
    if value == '':
        return True
    return len(value) < 10

def validate_FECHA(value):
    if value == '':
        return True
    try:
        datetime.strptime(value, '%d/%m/%Y') or datetime.strptime(value, '%d-%m-%Y')
        return True
    except ValueError:
        return False

def validate_RESULTADO(value):
    if value == '':
        return True
    allowed_words = ["Regular", "Libre", "Promociono", "Abandono", "Insuficiente"]
    return any(word in value for word in allowed_words)

def validate_NOTA(value):
    if value == '':
        return True
    try:
        i = int(value)
        return 0 <= i <= 10
    except ValueError:
        return len(value) == 1  # Accept single character

def validate_COD_NOSE(value):
    if value == '':
        return True
    try:
        f = float(value)
        return f.is_integer()
    except ValueError:
        return False

def validate_COD_NOSE_2(value):
    return value == '' or value in ['R', 'P']

# Get the path to the script's directory
script_dir = os.getcwd()

# Define paths relative to the script's location
input_file = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_02.csv')
output_good = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_03.csv')
output_bad = os.path.join(script_dir, 'data-private/CURSADA_HISTORICA_03_con_errores.csv')

with open(input_file, 'r', encoding='utf-8') as infile, \
        open(output_good, 'w', encoding='utf-8', newline='') as goodfile, \
        open(output_bad, 'w', encoding='utf-8', newline='') as badfile:
    reader = csv.reader(infile)
    good_writer = csv.writer(goodfile)
    bad_writer = csv.writer(badfile)

    header = next(reader)
    good_writer.writerow(header)
    # Add a new header for the error column in the bad file
    bad_writer.writerow(header + ['Errores en columnas'])

    for row in reader:
        # Skip empty lines
        if not row:
            continue
        if len(row) != 13:
            row_with_error_info = row + ['Número incorrecto de columnas']
            bad_writer.writerow(row_with_error_info)
            continue

        # Validate each field
        field_validations = [
            validate_ID(row[0]),           # Column 1
            validate_COD_CARRERA(row[1]),  # Column 2
            validate_ANIO(row[3]),         # Column 4
            validate_CUATRIMESTRE(row[4]), # Column 5
            validate_COD_MATERIA(row[5]),  # Column 6
            validate_COD_NOSE(row[7]),     # Column 8
            validate_COD_NOSE_2(row[8]),   # Column 9
            validate_NOTA(row[9]),         # Column 10
            validate_FECHA(row[10]),       # Column 11
            validate_FECHA(row[11]),       # Column 12
            validate_RESULTADO(row[12]),   # Column 13
        ]

        if not all(field_validations):
            validation_column_indices = [1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13]
            failed_columns = [
                validation_column_indices[i]
                for i, valid in enumerate(field_validations) if not valid
            ]
            failed_columns_str = ','.join(map(str, failed_columns))
            row_with_error_info = row + [failed_columns_str]
            bad_writer.writerow(row_with_error_info)
        else:
            good_writer.writerow(row)