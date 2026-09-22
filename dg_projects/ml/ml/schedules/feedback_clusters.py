from dagster import DefaultScheduleStatus, ScheduleDefinition

# Weekly, Monday 06:00 UTC -- a default, not a value calibrated on real data.
# is_promoted=True: this is the production launcher, so it opts in explicitly
# (feedback_clusters' own default is False -- fail closed for any other launch).
feedback_clusters_schedule = ScheduleDefinition(
    name="feedback_clusters_weekly_schedule",
    job_name="feedback_clusters_job",
    cron_schedule="0 6 * * 1",
    execution_timezone="Etc/UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"feedback_clusters": {"config": {"is_promoted": True}}}},
)
