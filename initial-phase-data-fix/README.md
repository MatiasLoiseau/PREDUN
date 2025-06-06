# Initial Phase: Dataset Preparation and Transformation Process

This document provides step-by-step instructions for processing a dataset from its raw format to a cleaned and transformed format, ready for EDA and further steps. The process involves three stages: reading and filtering the raw data, standardizing column values, and transforming specific columns for better usability.

---

## 1. Student History

### Step 1.1: Reading and Filtering the Dataset

The first step is to read the raw data from a text file, filter valid rows, and save the data to a CSV file.

- **File**: `history/01_fix_data.py`

#### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA.txt`
- **Output File**: `data-private/CURSADA_HISTORICA.csv`
- **Key Operations**:
  1. Define column names for the dataset.
  2. Read the text file with a specific encoding (`ISO-8859-15`).
  3. Filter rows with exactly 13 columns separated by `|`.
  4. Convert filtered rows into a pandas DataFrame.
  5. Save the DataFrame to a CSV file.

---

### Step 1.2: Standardizing Column Values

The second step involves mapping and standardizing the values in the `TIPO_CURSADA` column.

- **File**: `history/02_fix_data.py`

#### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA.csv`
- **Output File**: `data-private/CURSADA_HISTORICA_02.csv`
- **Key Operations**:
  1. Load the initial CSV file.
  2. Map verbose or inconsistent `TIPO_CURSADA` values to standardized short codes (e.g., `'1° cuatrimestre'` to `'1C'`).
  3. Save the modified DataFrame to a new CSV file.

---

### Step 1.3: Transforming Numeric Columns

The final step adjusts the `NOTA` column to replace categorical grades with numeric values.

- **File**: `history/03_fix_data.py`

#### Code Overview

- **Input File**: `data-private/CURSADA_HISTORICA_02.csv`
- **Output File**: `data-private/CURSADA_HISTORICA_final.csv`
- **Key Operations**:
  1. Load the standardized CSV file.
  2. Replace categorical grades (`'A'` and `'R'`) with numeric equivalents (`7` and `1`).
  3. Convert the `NOTA` column to a float type.
  4. Save the modified DataFrame to a final CSV file.

---

## 2. Converting Excel Data to CSV Format

The fourth step involves reading data from an Excel file and converting it to a CSV file for easier processing and analysis.

- **File**: `ransform_xlsx_to_csv.py`

#### Code Overview

- **Input File**: `data-private/CENSALES.xlsx`
- **Output File**: `data-private/CENSALES.csv`
- **Key Operations**:
  1. Load the Excel file into a pandas DataFrame using `read_excel`.
  2. Save the loaded data to a CSV file using `to_csv`, ensuring no index column is added.
  3. Set the encoding to `utf-8` for compatibility with most systems.
  4. Handle potential file errors gracefully with exception handling.

---

## Student List

### 3.1: Cleaning and Transforming the Student List Data

The fifth step focuses on cleaning and transforming a dataset of student information from a CSV file. This ensures the data is well-structured and ready for further analysis.

- **File**: `students/01_clean_student_list.py`

#### Code Overview

- **Input File**: `data-private/listado_alumnos.csv`
- **Output File**: `data-private/listado_alumnos_02.csv`
- **Key Operations**:
  1. **Drop Rows with Missing Values**: Ensures all rows have a valid `legajo` (student ID) by removing any rows where it is null.
  2. **Remove Unwanted Columns**: Drops unnecessary or placeholder columns (`Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`), if they exist, to clean the dataset.
  3. **Rename Columns**: Updates column names for better clarity and usability:
    - `codigo` → `codigo_carrera`
    - `nombre-2` → `nombre_carrera`
    - `codigo-2` → `codigo_pertenece`
    - `nombre-3` → `nombre_pertenece`
  4. **Save the Cleaned Data**: Writes the cleaned and transformed DataFrame to a new CSV file (`listado_alumnos_final.csv`).

---

### Step 3.2: Validating and Correcting the Student List Data

The sixth step ensures that specific fields in the cleaned student list dataset meet the required criteria for consistency and correctness. Rows that fail to meet these conditions are removed.

- **File**: `students/02_clean_student_list.py`

#### Code Overview

- **Input File**: `data-private/listado_alumnos_final.csv`
- **Output File**: `data-private/listado_alumnos_final_final.csv`
- **Key Operations**:
  1. **Validate `regular` Column**: Retain rows where the `regular` field contains only `'N'` or `'S'`.
  2. **Validate `anio_academico` Column**: Ensure that the `anio_academico` field contains only valid 4-digit years.
  3. **Filter `nombre_pertenece` Column**: Remove rows where the `nombre_pertenece` field contains invalid values:
    - `'D SA'`, `'D CA'`, `'D CS'`, `'D PT'`, or `'UA AC'`.
  4. **Validate `fecha_inscripcion` Column**: Ensure the `fecha_inscripcion` field follows the format `'YYYY-MM-DD HH:MM:SS'`.
  5. **Save the Cleaned Data**: Write the validated and corrected dataset to a new CSV file (`listado_alumnos_final.csv`).

---
