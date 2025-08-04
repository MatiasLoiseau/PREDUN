from dagster import RunRequest, SkipReason, sensor
from dagster_dbt import get_asset_key_for_model
from .jobs import refresh_canonical
from .assets import dbt_project_assets

PANEL_KEY = get_asset_key_for_model(
    dbt_assets=[dbt_project_assets],
    model_name="student_panel",
)

@sensor(job=refresh_canonical, minimum_interval_seconds=3600)
def new_period_sensor(context):
    records = context.instance.get_event_log_records(
        asset_key=PANEL_KEY,
        limit=1,
    )
    if not records:
        return SkipReason("student_panel aún no se ha materializado.")

    last_run_id = records[0].run_id
    if context.instance.has_run(last_run_id):
        return RunRequest(
            run_key=last_run_id,
            run_config={},
            tags={"trigger": "new_panel_period"},
        )

    return SkipReason("Esperando un nuevo student_panel.")