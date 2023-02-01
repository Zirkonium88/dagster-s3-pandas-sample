from dagster import Definitions
from .jobs.load_s3_job import load_s3
from .schedules.load_s3_schedule import load_s3_schedule

defs = Definitions(
    schedules=[load_s3_schedule],
    jobs=[load_s3]
)
