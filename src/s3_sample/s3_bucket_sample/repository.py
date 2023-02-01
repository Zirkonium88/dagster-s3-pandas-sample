from dagster import repository
from .jobs.load_s3_job import load_s3
from .schedules.load_s3_schedule import load_s3_schedule


@repository
def s3_sample_repository():
    return [load_s3, load_s3_schedule]