# Documentation about MLflow scripts

## database_init.py

### Overview
`database_init.py` is designed to automate the process of uploading CSV files into a PostgreSQL database while tracking metadata and performance metrics using MLflow.

### Features
- Loads environment variables for database configuration using `dotenv`.
- Reads CSV files and uploads them to PostgreSQL using `pandas` and `sqlalchemy`.
- Logs metadata, execution time, and data quality metrics in MLflow.
- Stores sample data and schema information for each uploaded table.

### Logging in MLflow
The script logs the following details in MLflow:
- **Data Version** (`DATA_VERSION_TAG`)
- **Database Connection Details**
- **Row Count per Table**
- **Upload Time per Table**
- **File Size in MB**
- **Missing Values Count**
- **Duplicate Rows Count**
- **Sample CSV Files**
- **Schema JSON Files**

### Notes
- The script replaces existing tables in PostgreSQL (`if_exists='replace'`). Modify this behavior if needed.
- MLflow should be running on `http://localhost:8002` or another configured tracking server.
- Ensure CSV files exist in `data-private/` before running the script.