from dagster import DynamicPartitionsDefinition, sensor, RunRequest
from pathlib import Path

academic_periods = DynamicPartitionsDefinition(name="academic_period")

@sensor(job_name="etl_period")
def discover_new_periods(context):
    root = Path("data-private")
    seen = set(context.instance.get_dynamic_partitions(academic_periods.name))
    found = {p.name for p in root.iterdir() if p.is_dir()}
    for new in found - seen:
        context.instance.add_dynamic_partition(academic_periods.name, new)
        yield RunRequest(partition_key=new)