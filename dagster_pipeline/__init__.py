"""
Dagster pipeline for stablecoin peg monitoring.
"""
from dagster import Definitions
from dagster_pipeline.jobs import stablecoin_daily_job, stablecoin_daily_schedule

defs = Definitions(
    jobs=[stablecoin_daily_job],
    schedules=[stablecoin_daily_schedule]
)

