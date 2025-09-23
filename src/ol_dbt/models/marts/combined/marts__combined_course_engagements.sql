with
    combined_course_activities_daily as (
        select
            '{{ var("mitxonline") }}' as platform,
            user_username,
            courserun_readable_id,
            courseactivity_date,
            courseactivity_num_events
        from {{ ref("int__mitxonline__user_courseactivities_daily") }}

        union all

        select
            '{{ var("edxorg") }}' as platform,
            user_username,
            courserun_readable_id,
            courseactivity_date,
            courseactivity_num_events
        from {{ ref("int__edxorg__mitx_user_courseactivities_daily") }}

        union all

        select
            '{{ var("mitxpro") }}' as platform,
            user_username,
            courserun_readable_id,
            courseactivity_date,
            courseactivity_num_events
        from {{ ref("int__mitxpro__user_courseactivities_daily") }}

        union all

        select
            '{{ var("residential") }}' as platform,
            user_username,
            courserun_readable_id,
            courseactivity_date,
            courseactivity_num_events
        from {{ ref("int__mitxresidential__user_courseactivities_daily") }}

    ),
    combined_play_video as (

        select
            '{{ var("mitxonline") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxonline__user_courseactivity_video") }}
        where useractivity_event_type = 'play_video'

        union all

        select
            '{{ var("edxorg") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__edxorg__mitx_user_courseactivity_video") }}
        where useractivity_event_type = 'play_video'

        union all

        select
            '{{ var("mitxpro") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxpro__user_courseactivity_video") }}
        where useractivity_event_type = 'play_video'

        union all

        select
            '{{ var("residential") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxresidential__user_courseactivity_video") }}
        where useractivity_event_type = 'play_video'
    ),
    combined_problem_submitted as (

        select
            '{{ var("mitxonline") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxonline__user_courseactivity_problemsubmitted") }}

        union all

        select
            '{{ var("edxorg") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__edxorg__mitx_user_courseactivity_problemsubmitted") }}

        union all

        select
            '{{ var("mitxpro") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxpro__user_courseactivity_problemsubmitted") }}

        union all

        select
            '{{ var("residential") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxresidential__user_courseactivity_problemsubmitted") }}
    ),
    combined_discussion as (

        select
            '{{ var("mitxonline") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxonline__user_courseactivity_discussion") }}
        -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
        where useractivity_event_type like 'edx.forum.%.created'

        union all

        select
            '{{ var("edxorg") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__edxorg__mitx_user_courseactivity_discussion") }}
        -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
        where useractivity_event_type like 'edx.forum.%.created'

        union all

        select
            '{{ var("mitxpro") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxpro__user_courseactivity_discussion") }}
        -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
        where useractivity_event_type like 'edx.forum.%.created'

        union all

        select
            '{{ var("residential") }}' as platform,
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date
        from {{ ref("int__mitxresidential__user_courseactivity_discussion") }}
        -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
        where useractivity_event_type like 'edx.forum.%.created'
    ),
    combined_users as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by user_username, platform order by openedx_user_id asc nulls last
                    ) as row_num
                from {{ ref("int__combined__users") }}
            )
        where row_num = 1
    ),
    combined_runs as (select * from {{ ref("int__combined__course_runs") }}),
    combined_play_video_daily as (
        select platform, user_username, courserun_readable_id, courseactivity_date, count(*) as num_video_played
        from combined_play_video
        group by platform, user_username, courserun_readable_id, courseactivity_date
    ),
    combined_problem_submitted_daily as (
        select platform, user_username, courserun_readable_id, courseactivity_date, count(*) as num_problem_submitted
        from combined_problem_submitted
        group by platform, user_username, courserun_readable_id, courseactivity_date
    ),
    combined_discussion_daily as (
        select
            platform, user_username, courserun_readable_id, courseactivity_date, count(*) as num_discussion_participated
        from combined_discussion
        group by platform, user_username, courserun_readable_id, courseactivity_date
    )

select
    combined_course_activities_daily.platform,
    combined_users.user_hashed_id,
    combined_course_activities_daily.user_username,
    combined_course_activities_daily.courserun_readable_id,
    combined_course_activities_daily.courseactivity_date,
    combined_course_activities_daily.courseactivity_num_events as num_events,
    combined_problem_submitted_daily.num_problem_submitted,
    combined_play_video_daily.num_video_played,
    combined_discussion_daily.num_discussion_participated,
    combined_users.user_full_name,
    combined_users.user_email,
    combined_users.user_address_country as user_country_code,
    combined_users.user_highest_education,
    combined_users.user_gender,
    combined_runs.courserun_title,
    combined_runs.course_readable_id,
    combined_runs.course_number,
    combined_runs.courserun_is_current,
    combined_runs.courserun_start_on,
    combined_runs.courserun_end_on
from combined_course_activities_daily
inner join
    combined_runs
    on combined_course_activities_daily.courserun_readable_id = combined_runs.courserun_readable_id
    and combined_course_activities_daily.platform = combined_runs.platform
left join
    combined_users
    on combined_course_activities_daily.user_username = combined_users.user_username
    and combined_course_activities_daily.platform = combined_users.platform
left join
    combined_problem_submitted_daily
    on combined_course_activities_daily.courseactivity_date = combined_problem_submitted_daily.courseactivity_date
    and combined_course_activities_daily.courserun_readable_id = combined_problem_submitted_daily.courserun_readable_id
    and combined_course_activities_daily.user_username = combined_problem_submitted_daily.user_username
left join
    combined_play_video_daily
    on combined_course_activities_daily.courseactivity_date = combined_play_video_daily.courseactivity_date
    and combined_course_activities_daily.courserun_readable_id = combined_play_video_daily.courserun_readable_id
    and combined_course_activities_daily.user_username = combined_play_video_daily.user_username
left join
    combined_discussion_daily
    on combined_course_activities_daily.courseactivity_date = combined_discussion_daily.courseactivity_date
    and combined_course_activities_daily.courserun_readable_id = combined_discussion_daily.courserun_readable_id
    and combined_course_activities_daily.user_username = combined_discussion_daily.user_username
