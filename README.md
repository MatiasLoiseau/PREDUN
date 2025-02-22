# Predicting Student Dropout Rates at UNDAV Using MLOps

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)[![Stable Release](https://img.shields.io/badge/development-v0.0.1-brightgreen.svg)](https://github.com/your-repo/releases)
<!--[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/release/python-380/)-->

PREDUN is an acronym for "Predicción de Deserción Universitaria en UNDAV".

## Project Overview

This project aims to develop and deploy machine learning models to predict student dropout rates at the Universidad Nacional de Avellaneda (UNDAV). By utilizing MLOps principles, the goal is to create a scalable, automated, and reproducible system that can predict which students are at risk of dropping out, thereby enabling timely interventions by the university administration.

## To-Do List

- Create EL process with Airbyte
- Perform transformations with DBT
- Orchestrate with Dagster
- Integrate MLFlow

## Dataset Information

The dataset used in this project contains academic records for students enrolled at UNDAV. Each row represents a unique record associated with a student's course enrollment or exam result. Key attributes are as follows:

- **ID**: Fictitious identifier for each student, ensuring privacy.
- **COD_CARRERA**: Numeric code identifying the academic program or course the student is registered in.
- **NOM_CARRERA**: Name of the academic program or course.
- **ANIO**: Year of registration for the course or exam.
- **TIPO_CURSADA**: Type of course at the time of registration, indicating the mode or type of study (e.g., regular, intensive).
- **COD_MATERIA**: Subject code, identifying specific subjects within a course.
- **NOM_MATERIA**: Name of the subject or course module.
- **NRO_ACTA**: Record number associated with a specific academic transaction or update.
- **ORIGEN**: Origin or source of the record, possibly indicating the administrative body or process involved.
- **NOTA**: Grade or score achieved by the student in the course or exam.
- **FECHA**: Date the record was created or the exam/course was completed.
- **FECHA_VIGENCIA**: Expiration or validity date of the record.
- **RESULTADO**: Outcome of the course or exam, such as "promociono" (passed) or "no promociono" (failed), representing academic achievement status.

### Data Mapping Dictionaries

To prepare the dataset for machine learning, certain categorical values have been mapped to numeric or standardized codes as follows:

- **Mapping for TIPO_CURSADA Column**:
  - `1Cuatrimestre`: `1C`
  - `2Cuatrimestre`: `2C`
  - `Cursada de verano`: `V`
  - `Anual`: `A`
  - `1Trimestre`: `1T`
  - `2Trimestre`: `2T`
  - `3Trimestre`: `3T`
  - `Microcredito-EAD`: `MC`
  - `Mensual`: `M`

These mappings ensure consistency and facilitate model training by transforming categorical data into standardized numerical values.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more details.
