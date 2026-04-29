with user_roles as (
    select
        user_fk
        , platform_code
        , courserun_readable_id
        , {{ array_join('array_agg(courseaccess_role order by courseaccess_role)', ", ") }} as course_roles
    from {{ ref('bridge_user_courserun_role') }}
    group by user_fk, platform_code, courserun_readable_id
)

, combined_course_roles as (
    select
        ucr.platform_code as platform
        , case ucr.platform_code
            when 'mitxonline' then du.user_mitxonline_username
            when 'edxorg' then du.user_edxorg_username
            when 'mitxpro' then du.user_mitxpro_username
            when 'residential' then du.user_residential_username
        end as user_username
        , du.email as user_email
        , ucr.courserun_readable_id
        , ucr.course_roles
    from user_roles as ucr
    inner join {{ ref('dim_user') }} as du on ucr.user_fk = du.user_pk
)

, combined_course_runs as (
    select
        platform
        , courserun_readable_id
        , courserun_start_on
        , courserun_end_on
        , {{ is_courserun_current('courserun_start_on', 'courserun_end_on') }} as courserun_is_current
        -- courserun_created_on is not available in dim_course_run
        , null as courserun_created_on
    from {{ ref('dim_course_run') }}
    where is_current = true
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
    , combined_course_runs.courserun_created_on
    , combined_course_roles.course_roles
    , course_activities.last_course_activity_date
from combined_course_roles
left join combined_course_runs
    on combined_course_runs.platform = combined_course_roles.platform
    and combined_course_runs.courserun_readable_id = combined_course_roles.courserun_readable_id
left join course_activities
    on course_activities.user_username = combined_course_roles.user_username
    and course_activities.courserun_readable_id = combined_course_roles.courserun_readable_id
