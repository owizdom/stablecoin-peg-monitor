"""
Dagster job to run daily stablecoin monitoring tasks.
"""
from dagster import job, schedule, ScheduleEvaluationContext, DefaultScheduleStatus
from dagster_pipeline.ops.chainlink_price_op import fetch_chainlink_price_op


@job
def stablecoin_daily_job():
    """
    Daily job to fetch Chainlink prices and load them into Snowflake.
    """
    fetch_chainlink_price_op()


@schedule(
    job=stablecoin_daily_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    default_status=DefaultScheduleStatus.RUNNING
)
def stablecoin_daily_schedule(context: ScheduleEvaluationContext):
    """
    Schedule to run stablecoin_daily_job every 24 hours at midnight UTC.
    """
    return {}

