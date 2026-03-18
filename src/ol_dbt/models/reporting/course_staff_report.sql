with combined_course_roles as (
    select
        platform
        , user_username
        , user_email
        , courserun_readable_id
        ,  {{ array_join('array_agg(courseaccess_role order by courseaccess_role)', ", ") }} as course_roles
    from {{ ref('int__combined__user_course_roles') }}
    group by platform, user_username, user_email, courserun_readable_id
)

, combined_course_runs as (
    select * from {{ ref('int__combined__course_runs') }}
)

, course_activities as (
    select
        platform
        , user_username
        , courserun_readable_id
        , max(courseactivity_date) as last_course_activity_date
    from {{ ref('marts__combined_course_engagements') }}
    group by platform, user_username, courserun_readable_id
)

select
    combined_course_roles.platform
    , combined_course_roles.user_username
    , combined_course_roles.user_email
    , combined_course_roles.courserun_readable_id
    , combined_course_runs.courserun_start_on
    , combined_course_runs.courserun_end_on
    , combined_course_runs.courserun_is_current
    , combined_course_roles.course_roles
    , course_activities.last_course_activity_date
from combined_course_roles
left join combined_course_runs
    on combined_course_runs.platform = combined_course_roles.platform
    and combined_course_runs.courserun_readable_id = combined_course_roles.courserun_readable_id
left join course_activities
    on course_activities.platform = combined_course_roles.platform
    and course_activities.user_username = combined_course_roles.user_username
    and course_activities.courserun_readable_id = combined_course_roles.courserun_readable_id
