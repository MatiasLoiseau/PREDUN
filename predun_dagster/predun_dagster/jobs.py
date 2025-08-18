from dagster import define_asset_job, AssetSelection

refresh_canonical = define_asset_job(
    name="refresh_canonical",
    selection=AssetSelection.keys(
        ["canonical", "alumnos"],
        ["canonical", "cursada_historica"],
        ["canonical", "porcentaje_avance"],
        ["marts", "student_status"],
        ["marts", "student_panel"],
    ),
)

train_ml_model = define_asset_job(
    name="train_ml_model",
    selection=AssetSelection.assets("train_student_dropout_model"),
)