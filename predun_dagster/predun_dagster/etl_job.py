from dagster import graph
from .partitions import academic_periods
from .etl_ops import clean_files, ingest_to_staging, refresh_canonical

@graph
def etl_graph():
    p = clean_files()
    p2 = ingest_to_staging(p)
    refresh_canonical(p2)

etl_period = etl_graph.to_job(
    name="etl_period",
    partitions_def=academic_periods,
    config={"ops": {"clean_files": {"config": {"period": {"expr": "partition_key"}}}}},
)