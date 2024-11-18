# Initial Phase: Dataset Preparation and Transformation Process

This document provides step-by-step instructions for processing a dataset from its raw format to a cleaned and transformed format, ready for EDA and further steps. The process involves three stages: reading and filtering the raw data, standardizing column values, and transforming specific columns for better usability.

---

## Step 1: Reading and Filtering the Dataset

The first step is to read the raw data from a text file, filter valid rows, and save the data to a CSV file.

- **File**: `01_fix_data.py`

### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA.txt`
- **Output File**: `data-private/CURSADA_HISTORICA.csv`
- **Key Operations**:
  1. Define column names for the dataset.
  2. Read the text file with a specific encoding (`ISO-8859-15`).
  3. Filter rows with exactly 13 columns separated by `|`.
  4. Convert filtered rows into a pandas DataFrame.
  5. Save the DataFrame to a CSV file.

---

## Step 2: Standardizing Column Values

The second step involves mapping and standardizing the values in the `TIPO_CURSADA` column.

- **File**: `02_fix_data.py`

### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA.csv`
- **Output File**: `data-private/CURSADA_HISTORICA_02.csv`
- **Key Operations**:
  1. Load the initial CSV file.
  2. Map verbose or inconsistent `TIPO_CURSADA` values to standardized short codes (e.g., `'1° cuatrimestre'` to `'1C'`).
  3. Save the modified DataFrame to a new CSV file.

---

## Step 3: Transforming Numeric Columns

The final step adjusts the `NOTA` column to replace categorical grades with numeric values.

- **File**: `03_fix_data.py`

### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA_02.csv`
- **Output File**: `data-private/CURSADA_HISTORICA_final.csv`
- **Key Operations**:
  1. Load the standardized CSV file.
  2. Replace categorical grades (`'A'` and `'R'`) with numeric equivalents (`7` and `1`).
  3. Convert the `NOTA` column to a float type.
  4. Save the modified DataFrame to a final CSV file.

---

## Step 4: Converting Excel Data to CSV Format

The fourth step involves reading data from an Excel file and converting it to a CSV file for easier processing and analysis.

- **File**: `04_transform_xlsx_to_csv.py`

### Code Overview

- **Input File**: `data-private/CENSALES.xlsx`
- **Output File**: `data-private/CENSALES.csv`
- **Key Operations**:
  1. Load the Excel file into a pandas DataFrame using `read_excel`.
  2. Save the loaded data to a CSV file using `to_csv`, ensuring no index column is added.
  3. Set the encoding to `utf-8` for compatibility with most systems.
  4. Handle potential file errors gracefully with exception handling.

---