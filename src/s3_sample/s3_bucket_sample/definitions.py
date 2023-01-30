
from dagster import Definitions

try:
    from s3_bucket_sample.jobs.load_s3_job import load_s3
    from s3_bucket_sample.schedules.load_s3_schedule import load_s3_schedule
except ModuleNotFoundError:
    from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import load_s3
    from src.s3_sample.s3_bucket_sample.schedules.load_s3_schedule import load_s3_schedule

defs = Definitions(
    schedules=[load_s3_schedule],
    jobs=[load_s3]
)
