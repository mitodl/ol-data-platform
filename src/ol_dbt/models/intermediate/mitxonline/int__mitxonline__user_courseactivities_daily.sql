with
    course_activities as (
        select *
        from {{ ref("stg__mitxonline__openedx__tracking_logs__user_activity") }}
        where courserun_readable_id is not null
    ),
    daily_activities_stats as (
        select
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date,
            count(*) as courseactivity_num_events
        from course_activities
        group by user_username, courserun_readable_id, date(from_iso8601_timestamp(useractivity_timestamp))
    )

select *
from daily_activities_stats
