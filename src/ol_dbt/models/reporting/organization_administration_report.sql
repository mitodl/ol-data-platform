{{ config (
    materialized='table',
    properties= {
      "format": "'PARQUET'",
      "partitioning": "ARRAY['organization_key']",
    }
)
}}

with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, user_course_roles as (
    select * from {{ ref('int__combined__user_course_roles') }}
)

, chatbot_events as (
    select * from {{ ref('tfact_chatbot_events') }}
)

, video_events as (
    select * from {{ ref('tfact_video_events') }}
)

, problem_events as (
    select * from {{ ref('tfact_problem_events') }}
)

, discussion_events as (
    select * from {{ ref('tfact_discussion_events') }}
)

, navigation_events as (
    select * from {{ ref('tfact_course_navigation_events') }}
)

, users as (
    select * from {{ ref('dim_user') }}
)

, enrollment as (
    select * from {{ ref('tfact_enrollment') }}
    where platform = 'mitxonline' and enrollment_type = 'course'
)

, contract as (
    select * from {{ ref('dim_contract') }}
)

, organization as (
    select * from {{ ref('dim_organization') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
    where is_current = true
)

-- export_b2b_organization_data ships each organization the rows with its
-- organization_key, so a learner's organization comes from the contract they enrolled
-- under and never from the run alone: a run can be offered under several contracts,
-- and to learners outside any of them.
, enrollment_organization as (
    select distinct
        enrollment_detail.courserun_readable_id
        , enrollment_detail.user_email
        , organization.organization_key
        , organization.organization_name
    from enrollment_detail
    inner join enrollment
        on cast(enrollment_detail.courserunenrollment_id as varchar) = enrollment.enrollment_id
    inner join contract
        on enrollment.contract_fk = contract.contract_pk
    inner join organization
        on contract.organization_fk = organization.organization_pk
    where enrollment_detail.platform = '{{ var("mitxonline") }}'
)

-- Runs offered under a contract. Their learners without one get no organization, not
-- the course-role fallback.
, contract_courseruns as (
    select distinct course_run.courserun_readable_id
    from {{ ref('bridge_organization_courserun') }} as organization_courserun
    inner join course_run
        on organization_courserun.courserun_fk = course_run.courserun_pk
)

, org_field as (
    select
        distinct courserun_readable_id
        , organization
    from user_course_roles
)

, certificate_org_data as (
    select
        distinct courserun_readable_id
        , user_email
        , cast(substring(courseruncertificate_created_on, 1, 10) as date) as certificate_created_date
    from enrollment_detail
    where courseruncertificate_is_earned = true
)

, enroll_data as (
    select
        courserun_readable_id
        , max(courserun_title) as courserun_title
        , user_email
    from enrollment_detail
    group by
        courserun_readable_id
        , user_email
)

, enroll_activity as (
    select
        user_email
        , courserun_readable_id
        , min(cast(substring(courserunenrollment_created_on, 1, 10) as date)) as enroll_date
    from enrollment_detail
    where courserunenrollment_is_active
    group by
        user_email
        , courserun_readable_id
)


, chatbot_data as (
    select
        users.email as user_email
        , cast(chatbot_events.event_timestamp as date) as activity_date
        , chatbot_events.courserun_readable_id
        , count(distinct {{ dbt.concat(["chatbot_events.session_id", "chatbot_events.block_id"]) }}) as chatbot_used_count
    from chatbot_events
    inner join users
        on chatbot_events.user_fk = users.user_pk
    where chatbot_events.event_type = 'ol_openedx_chat.drawer.submit'
    group by
        users.email
        , cast(chatbot_events.event_timestamp as date)
        , chatbot_events.courserun_readable_id
)

, video_data as (
    select
        users.email as user_email
        , cast(video_events.event_timestamp as date) as activity_date
        , video_events.courserun_readable_id
        , count(distinct video_events.video_block_fk) as videos_watched
    from video_events
    inner join users
        on video_events.user_fk = users.user_pk
    where video_events.event_type = 'play_video'
    group by
        users.email
        , cast(video_events.event_timestamp as date)
        , video_events.courserun_readable_id
)

, problem_data as (
    select
        users.email as user_email
        , cast(problem_events.event_timestamp as date) as activity_date
        , problem_events.courserun_readable_id
        , count(distinct problem_events.problem_block_fk) as problems_count
    from problem_events
    inner join users
        on problem_events.user_fk = users.user_pk
    group by
        users.email
        , cast(problem_events.event_timestamp as date)
        , problem_events.courserun_readable_id
)

, navigation_data as (
    select
        users.email as user_email
        , cast(navigation_events.event_timestamp as date) as activity_date
        , navigation_events.courserun_readable_id
        , count(*) as navigation_count
    from navigation_events
    inner join users
        on navigation_events.user_fk = users.user_pk
    group by
        users.email
        , cast(navigation_events.event_timestamp as date)
        , navigation_events.courserun_readable_id
)

, discussion_data as (
    select
        users.email as user_email
        , cast(discussion_events.event_timestamp as date) as activity_date
        , discussion_events.courserun_readable_id
        , count(*) as discussion_count
    from discussion_events
    inner join users
        on discussion_events.user_fk = users.user_pk
    group by
        users.email
        , cast(discussion_events.event_timestamp as date)
        , discussion_events.courserun_readable_id
)

, combined_data as (
    select
        distinct user_email
        , activity_date
        , courserun_readable_id
        , chatbot_used_count
        , 0 as certificate_count
        , 0 as videos_watched
        , 0 as problems_count
        , 0 as navigation_count
        , 0 as discussion_count
        , 0 as enrolled_count
    from chatbot_data

    union

    select
        distinct user_email
        , certificate_created_date as activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 1 as certificate_count
        , 0 as videos_watched
        , 0 as problems_count
        , 0 as navigation_count
        , 0 as discussion_count
        , 0 as enrolled_count
    from certificate_org_data

    union

    select
        distinct user_email
        , activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 0 as certificate_count
        , videos_watched
        , 0 as problems_count
        , 0 as navigation_count
        , 0 as discussion_count
        , 0 as enrolled_count
    from video_data

    union

    select
        distinct user_email
        , activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 0 as certificate_count
        , 0 as videos_watched
        , problems_count
        , 0 as navigation_count
        , 0 as discussion_count
        , 0 as enrolled_count
    from problem_data

    union

    select
        distinct user_email
        , activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 0 as certificate_count
        , 0 as videos_watched
        , 0 as problems_count
        , navigation_count
        , 0 as discussion_count
        , 0 as enrolled_count
    from navigation_data

    union

    select
        distinct user_email
        , activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 0 as certificate_count
        , 0 as videos_watched
        , 0 as problems_count
        , 0 as navigation_count
        , discussion_count
        , 0 as enrolled_count
    from discussion_data

    union

    select
        distinct user_email
        , enroll_date as activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 0 as certificate_count
        , 0 as videos_watched
        , 0 as problems_count
        , 0 as navigation_count
        , 0 as discussion_count
        , 1 as enrolled_count
    from enroll_activity
)

, activity_day_data as (
    select
        user_email
        , activity_date
        , courserun_readable_id
        , sum(chatbot_used_count) as chatbot_used_count
        , sum(videos_watched) as videos_watched
        , sum(problems_count) as problems_count
        , sum(navigation_count) as navigation_count
        , sum(discussion_count) as discussion_count
        , max(certificate_count) as certificate_count
        , max(enrolled_count) as enrolled_count
    from combined_data
    group by
        user_email
        , activity_date
        , courserun_readable_id
)


select
    enroll_data.courserun_readable_id
    , enroll_data.courserun_title
    , enroll_data.user_email
    , activity_day_data.enrolled_count
    , coalesce(enrollment_organization.organization_key, org_field.organization) as organization_key
    , enrollment_organization.organization_name
    , activity_day_data.activity_date
    , substring(cast(activity_day_data.activity_date as varchar), 1, 7) as activity_year_and_month
    , activity_day_data.chatbot_used_count
    , activity_day_data.certificate_count
    , activity_day_data.videos_watched
    , activity_day_data.problems_count
    , case when activity_day_data.navigation_count > 0
        or activity_day_data.discussion_count > 0
        or activity_day_data.videos_watched > 0
        or activity_day_data.problems_count > 0
        or activity_day_data.chatbot_used_count > 0
        or activity_day_data.certificate_count > 0
        then 1 else 0 end as active_count
from enroll_data
left join contract_courseruns
    on enroll_data.courserun_readable_id = contract_courseruns.courserun_readable_id
left join org_field
    on
        enroll_data.courserun_readable_id = org_field.courserun_readable_id
        and contract_courseruns.courserun_readable_id is null
left join activity_day_data
    on
        enroll_data.user_email = activity_day_data.user_email
        and enroll_data.courserun_readable_id = activity_day_data.courserun_readable_id
left join enrollment_organization
    on
        enroll_data.user_email = enrollment_organization.user_email
        and enroll_data.courserun_readable_id = enrollment_organization.courserun_readable_id
