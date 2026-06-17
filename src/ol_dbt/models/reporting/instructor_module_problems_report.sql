with enrollment as (
    select * from {{ ref('tfact_enrollment') }}
)

, user as (
    select * from {{ ref('dim_user') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
    where is_current = true
)

, problem as (
    select * from {{ ref('dim_problem') }}
)

, problem_events as (
    select * from {{ ref('tfact_problem_events') }}
)

, course_content as (
    select * from {{ ref('dim_course_content') }}
)

, course as (
    select * from {{ ref('dim_course') }}
    where is_current = true
)

, organization_courserun as (
    select * from {{ ref('bridge_organization_courserun') }}
)

, organization as (
    select * from {{ ref('dim_organization') }}
)

select
    user.email as user_email
    , user.full_name
    , course_run.courserun_readable_id
    , section.block_title as section_title
    , subsection.block_title as subsection_title
    , problem.problem_name
    , problem_events.attempt
    , problem_events.success
    , problem_events.grade
    , enrollment.platform
    , course.course_title
    , organization.organization_name
from enrollment
inner join user
    on enrollment.user_fk = user.user_pk
inner join course_run
    on enrollment.courserun_fk = course_run.courserun_pk
inner join problem
    on
        course_run.courserun_readable_id = problem.courserun_readable_id
        and course_run.platform = problem.platform
left join problem_events
    on
        problem.problem_block_pk = problem_events.problem_block_fk
        and problem.platform = problem_events.platform
        and problem.courserun_readable_id = problem_events.courserun_readable_id
        and user.user_pk = problem_events.user_fk
left join course_content as content
    on
        problem.content_block_fk = content.content_block_pk
        and content.is_latest = true
left join course_content as section
    on
        content.chapter_block_id = section.block_id
        and section.is_latest = true
left join course_content as subsection
    on
        content.sequential_block_id = subsection.block_id
        and subsection.is_latest = true
left join course
    on course_run.course_fk = course.course_pk
left join organization_courserun
    on course_run.courserun_pk = organization_courserun.courserun_fk
left join organization
    on organization_courserun.organization_fk = organization.organization_pk
