
from dagster import repository

from s3_bucket_sample.jobs.s3_sample import load_s3
from s3_bucket_sample.jobs.s3_sample import load_s3_schedule


@repository
def s3_sample_repository():
    return [load_s3, load_s3_schedule]