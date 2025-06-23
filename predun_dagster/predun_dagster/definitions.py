from dagster import Definitions
from .etl_job import etl_period
from .partitions import discover_new_periods   # academic_periods

defs = Definitions(
    jobs=[etl_period],
    sensors=[discover_new_periods],
    # schedules=[weekly_etl],
)