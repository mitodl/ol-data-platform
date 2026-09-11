-- Grain: one row per (platform, user, course run, day) with any tracked course activity.
-- Keyed on user_fk, which organization_administration_report cannot offer: it groups
-- by dim_user.email, a coalesce across a person's accounts that moves when they edit
-- it. Rebuilt in full (the dimensional default) rather than incremental because
-- user_pk can still re-key, and an incremental table would keep old days under the
-- stale key.
-- Day boundary and metric definitions follow organization_administration_report,
-- except where noted below.
with video_days as (
    select
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date) as activity_date
        , count(distinct video_block_fk) as videos_played
    from {{ ref('tfact_video_events') }}
    where event_type = 'play_video'
      and user_fk is not null
    group by
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date)
)

-- problem_check only: showanswer is viewing a solution, not attempting the problem.
-- The report's problems_count counts both.
, problem_days as (
    select
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date) as activity_date
        , count(distinct problem_block_fk) as problems_attempted
    from {{ ref('tfact_problem_events') }}
    where event_type = 'problem_check'
      and user_fk is not null
    group by
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date)
)

, navigation_days as (
    select
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date) as activity_date
        , count(*) as navigation_events
    from {{ ref('tfact_course_navigation_events') }}
    where user_fk is not null
    group by
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date)
)

, discussion_days as (
    select
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date) as activity_date
        , count(*) as discussion_events
    from {{ ref('tfact_discussion_events') }}
    where user_fk is not null
    group by
        platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date)
)

-- Open edX chatbot events only, which all come from MITx Online's Open edX (see
-- tfact_chatbot_events). Canvas course ids are not Open edX course runs. A submit is
-- one interaction per (session, block); the coalesce keeps submits with no block_id,
-- which the report's `session_id || block_id` turns into null and drops.
, chatbot_days as (
    select
        'mitxonline' as platform
        , user_fk
        , courserun_readable_id
        , cast(event_timestamp as date) as activity_date
        , count(distinct concat(session_id, '|', coalesce(block_id, ''))) as chatbot_interactions
    from {{ ref('tfact_chatbot_events') }}
    where event_type = 'ol_openedx_chat.drawer.submit'
      and chatbot_source = 'open_edx'
      and courserun_readable_id is not null
      and user_fk is not null
    group by
        user_fk
        , courserun_readable_id
        , cast(event_timestamp as date)
)

, activity as (
    select
        platform, user_fk, courserun_readable_id, activity_date
        , videos_played, 0 as problems_attempted, 0 as navigation_events
        , 0 as discussion_events, 0 as chatbot_interactions
    from video_days
    union all
    select
        platform, user_fk, courserun_readable_id, activity_date
        , 0, problems_attempted, 0, 0, 0
    from problem_days
    union all
    select
        platform, user_fk, courserun_readable_id, activity_date
        , 0, 0, navigation_events, 0, 0
    from navigation_days
    union all
    select
        platform, user_fk, courserun_readable_id, activity_date
        , 0, 0, 0, discussion_events, 0
    from discussion_days
    union all
    select
        platform, user_fk, courserun_readable_id, activity_date
        , 0, 0, 0, 0, chatbot_interactions
    from chatbot_days
)

, activity_days as (
    select
        platform
        , user_fk
        , courserun_readable_id
        , activity_date
        , sum(videos_played) as videos_played
        , sum(problems_attempted) as problems_attempted
        , sum(navigation_events) as navigation_events
        , sum(discussion_events) as discussion_events
        , sum(chatbot_interactions) as chatbot_interactions
    from activity
    group by
        platform
        , user_fk
        , courserun_readable_id
        , activity_date
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

select
    {{ dbt_utils.generate_surrogate_key([
        'activity_days.platform',
        'activity_days.user_fk',
        'activity_days.courserun_readable_id',
        'activity_days.activity_date'
    ]) }} as activity_key
    , activity_days.user_fk
    , dim_course_run.courserun_pk as courserun_fk
    , activity_days.platform
    , activity_days.courserun_readable_id
    , activity_days.activity_date
    , activity_days.videos_played
    , activity_days.problems_attempted
    , activity_days.navigation_events
    , activity_days.discussion_events
    , activity_days.chatbot_interactions
from activity_days
left join dim_course_run
    on activity_days.courserun_readable_id = dim_course_run.courserun_readable_id
    and activity_days.platform = dim_course_run.platform
