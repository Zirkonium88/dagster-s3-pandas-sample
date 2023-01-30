from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.load_s3_job import load_s3

load_s3_schedule = ScheduleDefinition(
    name="s3_sample_routine",
    cron_schedule="0 0 * * *",
    job=load_s3,
    default_status=DefaultScheduleStatus.RUNNING,
)
