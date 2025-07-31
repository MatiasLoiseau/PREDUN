
from dagster import AssetSelection, define_asset_job

# Job que ejecuta los 4 assets de ingestion y permite pasar config
ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.assets(
        "format_history_data",
        "format_students",
        "format_percentage",
        "ingest_to_staging",
    ),
)
