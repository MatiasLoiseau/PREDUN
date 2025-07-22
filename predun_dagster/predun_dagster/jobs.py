from dagster import define_asset_job

full_pipeline_job = define_asset_job(
    name="full_student_dropout_pipeline",
    selection="*",
)