
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="s3sample"), cron_schedule="*/1 * * * *"
)


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    schedules=[daily_refresh_schedule],
)