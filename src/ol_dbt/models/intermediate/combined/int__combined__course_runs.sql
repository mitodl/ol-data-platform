with mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxonline_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, edxorg_runs as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, mitxpro_courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, mitxpro_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, bootcamps_courses as (
    select * from {{ ref('int__bootcamps__courses') }}
)

, bootcamps_runs as (
    select * from {{ ref('int__bootcamps__course_runs') }}
)

, micromasters_runs as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_courserun') }}
)

, emeritus_runs as (
    select distinct
        courserun_external_readable_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
    from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
)

, global_alumni_runs as (
    select distinct
        courserun_external_readable_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
    from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
)

, residential_runs as (
    select
        *
        , {{ extract_course_readable_id('courserun_readable_id') }} as course_readable_id
    from {{ ref('int__mitxresidential__courseruns') }}
)

, combined_runs as (
    select
        '{{ var("mitxonline") }}' as platform
        , mitx_courses.course_title
        , mitx_courses.course_readable_id
        , mitxonline_runs.courserun_title
        , mitxonline_runs.courserun_readable_id
        , mitxonline_runs.courserun_url
        , mitxonline_runs.courserun_start_on
        , mitxonline_runs.courserun_end_on
        , mitxonline_runs.courserun_upgrade_deadline
        , mitxonline_runs.courserun_is_live
        , case
            when
                mitxonline_runs.courserun_end_on is null
                and from_iso8601_timestamp(mitxonline_runs.courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(mitxonline_runs.courserun_start_on) <= current_date
                and from_iso8601_timestamp(mitxonline_runs.courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from mitxonline_runs
    left join mitx_courses on mitxonline_runs.course_id = mitx_courses.mitxonline_course_id
    where mitxonline_runs.courserun_platform = '{{ var("mitxonline") }}'

    union all

    select
        '{{ var("edxorg") }}' as platform
        , mitx_courses.course_title
        , mitx_courses.course_readable_id
        , edxorg_runs.courserun_title
        , edxorg_runs.courserun_readable_id
        , edxorg_runs.courserun_url
        , edxorg_runs.courserun_start_date as courserun_start_on
        , edxorg_runs.courserun_end_date as courserun_end_on
        , micromasters_runs.courserun_upgrade_deadline
        , null as courserun_is_live
        , case
            when
                edxorg_runs.courserun_end_date is null
                and from_iso8601_timestamp(edxorg_runs.courserun_start_date) <= current_date
                then true
            when
                from_iso8601_timestamp(edxorg_runs.courserun_start_date) <= current_date
                and from_iso8601_timestamp(edxorg_runs.courserun_end_date) > current_date
                then true
            else false
        end as courserun_is_current
    from edxorg_runs
    left join mitx_courses on edxorg_runs.course_number = mitx_courses.course_number
    left join micromasters_runs on edxorg_runs.courserun_readable_id = micromasters_runs.courserun_edxorg_readable_id

    union all

    select
        case
            when mitxpro_runs.platform_id = 2 then '{{ var("mitxpro") }}'
            else 'xPRO' || mitxpro_runs.platform_name
        end as platform_name
        , mitxpro_courses.course_title
        , mitxpro_courses.course_readable_id
        , mitxpro_runs.courserun_title
        , mitxpro_runs.courserun_readable_id
        , mitxpro_runs.courserun_url
        , mitxpro_runs.courserun_start_on
        , mitxpro_runs.courserun_end_on
        , null as courserun_upgrade_deadline
        , mitxpro_runs.courserun_is_live
        , case
            when
                mitxpro_runs.courserun_end_on is null
                and from_iso8601_timestamp(mitxpro_runs.courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(mitxpro_runs.courserun_start_on) <= current_date
                and from_iso8601_timestamp(mitxpro_runs.courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from mitxpro_runs
    left join mitxpro_courses on mitxpro_runs.course_id = mitxpro_courses.course_id

    union all

    select
        '{{ var("emeritus") }}' as platform
        , emeritus_runs.courserun_title as course_title
        , emeritus_runs.courserun_external_readable_id as course_readable_id
        , emeritus_runs.courserun_title
        , emeritus_runs.courserun_external_readable_id
        , null as courserun_url
        , emeritus_runs.courserun_start_on
        , emeritus_runs.courserun_end_on
        , null as courserun_upgrade_deadline
        , null as courserun_is_live
        , case
            when
                emeritus_runs.courserun_end_on is null
                and from_iso8601_timestamp(mitxpro_runs.courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(emeritus_runs.courserun_start_on) <= current_date
                and from_iso8601_timestamp(emeritus_runs.courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from emeritus_runs
    left join mitxpro_runs
        on emeritus_runs.courserun_external_readable_id = mitxpro_runs.courserun_external_readable_id
    where mitxpro_runs.courserun_external_readable_id is null

    union all
    -- any remaining global alumni runs that are not in mitxpro
    select
        '{{ var("global_alumni") }}' as platform
        , global_alumni_runs.courserun_title as course_title
        , global_alumni_runs.courserun_external_readable_id as course_readable_id
        , global_alumni_runs.courserun_title
        , global_alumni_runs.courserun_external_readable_id
        , null as courserun_url
        , global_alumni_runs.courserun_start_on
        , global_alumni_runs.courserun_end_on
        , null as courserun_upgrade_deadline
        , null as courserun_is_live
        , case
            when
                global_alumni_runs.courserun_end_on is null
                and from_iso8601_timestamp(global_alumni_runs.courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(global_alumni_runs.courserun_start_on) <= current_date
                and from_iso8601_timestamp(global_alumni_runs.courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from global_alumni_runs
    left join mitxpro_runs
        on global_alumni_runs.courserun_external_readable_id = mitxpro_runs.courserun_external_readable_id
    where mitxpro_runs.courserun_external_readable_id is null

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , bootcamps_courses.course_title
        , bootcamps_courses.course_readable_id
        , bootcamps_runs.courserun_title
        , bootcamps_runs.courserun_readable_id
        , null as courserun_url
        , bootcamps_runs.courserun_start_on
        , bootcamps_runs.courserun_end_on
        , null as courserun_upgrade_deadline
        , null as courserun_is_live
        , case
            when
                bootcamps_runs.courserun_end_on is null
                and from_iso8601_timestamp(bootcamps_runs.courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(bootcamps_runs.courserun_start_on) <= current_date
                and from_iso8601_timestamp(bootcamps_runs.courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from bootcamps_runs
    left join bootcamps_courses on bootcamps_runs.course_id = bootcamps_courses.course_id

    union all

    select
        '{{ var("residential") }}' as platform
        , courserun_title as course_title
        , course_readable_id
        , courserun_title
        , courserun_readable_id
        , null as courserun_url
        , courserun_start_on
        , courserun_end_on
        , null as courserun_upgrade_deadline
        , null as courserun_is_live
        , case
            when
                courserun_end_on is null
                and from_iso8601_timestamp(courserun_start_on) <= current_date
                then true
            when
                from_iso8601_timestamp(courserun_start_on) <= current_date
                and from_iso8601_timestamp(courserun_end_on) > current_date
                then true
            else false
        end as courserun_is_current
    from residential_runs
)

select * from combined_runs
