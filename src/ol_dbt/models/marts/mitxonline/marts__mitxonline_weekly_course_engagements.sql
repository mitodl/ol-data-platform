{% set week_start_date= '2022-01-02' %}

with timeseries as (
    select * from unnest(
        sequence(date '{{ week_start_date }}', current_date, interval '7' day)
    ) as t(week_start_date) -- noqa
)

, weekly_timeseries as (
    select
        week_start_date
        , date_add('day', 6, week_start_date) as week_end_date
    from timeseries
)

, course_activities_daily as (
    select * from {{ ref('int__mitxonline__user_courseactivities_daily') }}
)

, video as (
    select * from {{ ref('int__mitxonline__user_courseactivity_video') }}
)

, problem_submitted as (
    select * from {{ ref('int__mitxonline__user_courseactivity_problemsubmitted') }}
)

, discussion as (
    select * from {{ ref('int__mitxonline__user_courseactivity_discussion') }}
)

, course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)


, engaged_users as (
    select
        weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
        , course_activities_daily.courserun_readable_id
        , count(distinct course_activities_daily.user_username) as unique_users
    from weekly_timeseries
    inner join course_activities_daily
        on
            weekly_timeseries.week_start_date <= course_activities_daily.courseactivity_date
            and weekly_timeseries.week_end_date >= course_activities_daily.courseactivity_date
    group by
        course_activities_daily.courserun_readable_id
        , weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
)

, problem_submitted_users as (
    select
        weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
        , problem_submitted.courserun_readable_id
        , count(distinct problem_submitted.user_username) as unique_submitted_problem_users
    from weekly_timeseries
    left join problem_submitted
        on
            weekly_timeseries.week_start_date <= from_iso8601_timestamp(problem_submitted.useractivity_timestamp)
            and weekly_timeseries.week_end_date >= from_iso8601_timestamp(problem_submitted.useractivity_timestamp)
    group by
        problem_submitted.courserun_readable_id
        , weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
)

, played_video_users as (
    select
        weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
        , video.courserun_readable_id
        , count(distinct video.user_username) as unique_played_video_users
    from weekly_timeseries
    left join video
        on
            weekly_timeseries.week_start_date <= from_iso8601_timestamp(video.useractivity_timestamp)
            and weekly_timeseries.week_end_date >= from_iso8601_timestamp(video.useractivity_timestamp)
    where video.useractivity_event_type = 'play_video'
    group by
        video.courserun_readable_id
        , weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
)

, participated_discussion_users as (
    select
        weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
        , discussion.courserun_readable_id
        , count(distinct discussion.user_username) as unique_participated_discussion_users
    from weekly_timeseries
    left join discussion
        on
            weekly_timeseries.week_start_date <= from_iso8601_timestamp(discussion.useractivity_timestamp)
            and weekly_timeseries.week_end_date >= from_iso8601_timestamp(discussion.useractivity_timestamp)
    -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
    where discussion.useractivity_event_type like 'edx.forum.%.created'
    group by
        discussion.courserun_readable_id
        , weekly_timeseries.week_start_date
        , weekly_timeseries.week_end_date
)

select
    engaged_users.week_start_date
    , engaged_users.week_end_date
    , engaged_users.courserun_readable_id
    , course_runs.courserun_title
    , course_runs.course_number
    , course_runs.courserun_start_on
    , course_runs.courserun_end_on
    , engaged_users.unique_users
    , problem_submitted_users.unique_submitted_problem_users
    , played_video_users.unique_played_video_users
    , participated_discussion_users.unique_participated_discussion_users
from engaged_users
inner join course_runs
    on engaged_users.courserun_readable_id = course_runs.courserun_readable_id
left join problem_submitted_users
    on
        engaged_users.week_start_date = problem_submitted_users.week_start_date
        and engaged_users.week_end_date = problem_submitted_users.week_end_date
        and engaged_users.courserun_readable_id = problem_submitted_users.courserun_readable_id
left join played_video_users
    on
        engaged_users.week_start_date = played_video_users.week_start_date
        and engaged_users.week_end_date = played_video_users.week_end_date
        and engaged_users.courserun_readable_id = played_video_users.courserun_readable_id
left join participated_discussion_users
    on
        engaged_users.week_start_date = participated_discussion_users.week_start_date
        and engaged_users.week_end_date = participated_discussion_users.week_end_date
        and engaged_users.courserun_readable_id = participated_discussion_users.courserun_readable_id
order by
    engaged_users.week_start_date desc
