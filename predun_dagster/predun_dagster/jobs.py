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

# Entrenamiento multi-modelo + scoring (sin detección de drift)
complete_ml_pipeline = define_asset_job(
    name="complete_ml_pipeline",
    selection=AssetSelection.assets(
        "train_student_dropout_model",
        "predict_student_dropout_risk",
    ),
)

# Solo detección de drift (ejecutar luego de refresh_canonical)
drift_job = define_asset_job(
    name="drift_job",
    selection=AssetSelection.assets("detect_data_drift"),
)

# Ciclo completo de ML: drift → entrenamiento multi-modelo → scoring
# Ejecutar luego de refresh_canonical para tener student_panel actualizado.
full_cycle_job = define_asset_job(
    name="full_cycle_job",
    selection=AssetSelection.assets(
        "detect_data_drift",
        "train_student_dropout_model",
        "predict_student_dropout_risk",
    ),
)