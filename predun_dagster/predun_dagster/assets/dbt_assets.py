from dagster_dbt import dbt_assets
from ..constants import DBT_PROJECT_DIR

MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

@dbt_assets(manifest=str(MANIFEST_PATH))
def dbt_project_assets(context):
    yield from context.resources.dbt.cli(["run"],  context=context)
    yield from context.resources.dbt.cli(["test"], context=context)