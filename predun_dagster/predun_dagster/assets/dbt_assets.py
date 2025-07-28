from dagster_dbt import dbt_assets
from ..constants import DBT_PROJECT_DIR

MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

@dbt_assets(manifest=str(MANIFEST_PATH), required_resource_keys={"dbt"})
def dbt_project_assets(context):
    yield from context.resources.dbt.cli([
        "run", "--select", "canonical.*"
    ], context=context).stream()