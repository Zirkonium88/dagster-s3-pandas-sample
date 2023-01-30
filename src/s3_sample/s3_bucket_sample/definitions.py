
from dagster import Definitions

from s3_bucket_sample.jobs.s3_sample import load_s3, load_s3_schedule

defs = Definitions(
    schedules=[load_s3_schedule],
    jobs=[load_s3]
)
