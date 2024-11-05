# Initial Phase Data Fix

## Overview

This part of the repository contains a series of Python scripts designed to process and clean the `CURSADA_HISTORICA` data. The data represents historical course enrollment information, and the scripts perform the following tasks in sequence:

1. **Convert raw text data into a structured CSV format.**
2. **Clean the CSV by removing invalid IDs.**
3. **Validate data entries and separate valid and invalid rows.**
4. **Standardize and map specific fields to consistent formats.**
5. **Finalize the dataset by mapping course types and converting data types.**

By following this pipeline, you'll transform the raw data into a clean, structured dataset ready for analysis.

---

## Scripts and Their Functions

### 1. `fix_data.py`

- **Purpose:** Converts the raw text data file `CURSADA_HISTORICA.txt` into a structured CSV format `CURSADA_HISTORICA.csv`.
- **Functions:**
    - `replace_braces_with_pipe(text)`: Replaces `{` with `|` to standardize delimiters.
    - `remove_special_characters(text)`: Removes Latin special characters (e.g., accents).
    - `process_file(input_file, output_file)`: Processes the input file line by line, cleans it, and writes the structured data to a CSV file.
- **Output:** `data-private/CURSADA_HISTORICA.csv`

---

### 2. `fix_data_2.py`

- **Purpose:** Cleans the CSV file by removing rows where the `ID` field is not numeric.
- **Functions:**
    - `clean_csv(file_path, output_file_path)`: Reads the CSV, removes invalid rows, and saves the cleaned data.
- **Output:** `data-private/CURSADA_HISTORICA_02.csv`

---

### 3. `fix_data_3.py`

- **Purpose:** Validates each row in the CSV file based on predefined criteria, separates valid rows from invalid ones, and logs any errors.
- **Functions:**
    - Multiple `validate_*` functions for fields like `ID`, `COD_CARRERA`, `ANIO`, etc.
- **Process:**
    - Reads `CURSADA_HISTORICA_02.csv`.
    - Validates each field in every row.
    - Writes valid rows to `CURSADA_HISTORICA_03.csv`.
    - Writes invalid rows and error details to `CURSADA_HISTORICA_03_con_errores.csv`.
- **Outputs:**
    - `data-private/CURSADA_HISTORICA_03.csv` (valid data)
    - `data-private/CURSADA_HISTORICA_03_con_errores.csv` (invalid data with errors)

---

### 4. `fix_data_4.py`

- **Purpose:** Cleans and standardizes the `RESULTADO` column by mapping various textual representations to consistent numeric codes.
- **Functions:**
    - `clean_and_map_resultado(value)`: Cleans strings and maps them to predefined numeric codes.
- **Mappings:**
    - `regular` ➔ `1`
    - `promociono` ➔ `2`
    - `abandono` ➔ `3`
    - `libre` ➔ `4`
    - `insuficiente` ➔ `5`
    - `nopromociono` ➔ `6`
- **Output:** `data-private/CURSADA_HISTORICA_04.csv`

---

### 5. `fix_data_5.py`

- **Purpose:** Finalizes the dataset by mapping course types to standardized abbreviations and converting specific columns to integer data types.
- **Process:**
    - Maps `TIPO_CURSADA` values to abbreviations (e.g., `1#uatrimestre` to `1C`).
    - Converts columns like `ID`, `ANIO`, `NRO_ACTA`, and `RESULTADO` to integers.
- **Output:** `data-private/CURSADA_HISTORICA_05.csv`

---

## Data Files

- **Input Files:**
    - `data-private/CURSADA_HISTORICA.txt`: Raw data file.
- **Intermediate Files:**
    - `data-private/CURSADA_HISTORICA.csv`
    - `data-private/CURSADA_HISTORICA_02.csv`
    - `data-private/CURSADA_HISTORICA_03.csv`
    - `data-private/CURSADA_HISTORICA_03_con_errores.csv`
    - `data-private/CURSADA_HISTORICA_04.csv`
- **Final Output:**
    - `data-private/CURSADA_HISTORICA_05.csv`: Cleaned and finalized dataset.
