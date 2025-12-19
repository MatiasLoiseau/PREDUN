
from dagster import AssetSelection, define_asset_job

# Job que ejecuta solo los assets de formateo
format_data_job = define_asset_job(
    name="format_data_job",
    selection=AssetSelection.assets(
        "format_history_data",
        "format_students",
        "format_percentage",
    ),
)

# Job que ejecuta solo la ingestion a staging (depende de los assets de formateo)
ingest_to_staging_job = define_asset_job(
    name="ingest_to_staging_job",
    selection=AssetSelection.assets(
        "ingest_to_staging",
    ),
)
