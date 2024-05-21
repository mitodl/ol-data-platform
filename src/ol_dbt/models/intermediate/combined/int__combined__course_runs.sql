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
    left join mitx_courses on mitxonline_runs.course_number = mitx_courses.course_number
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

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , mitxpro_courses.course_title
        , mitxpro_courses.course_readable_id
        , mitxpro_runs.courserun_title
        , mitxpro_runs.courserun_readable_id
        , mitxpro_runs.courserun_url
        , mitxpro_runs.courserun_start_on
        , mitxpro_runs.courserun_end_on
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
        '{{ var("bootcamps") }}' as platform
        , bootcamps_courses.course_title
        , bootcamps_courses.course_readable_id
        , bootcamps_runs.courserun_title
        , bootcamps_runs.courserun_readable_id
        , null as courserun_url
        , bootcamps_runs.courserun_start_on
        , bootcamps_runs.courserun_end_on
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

)

select * from combined_runs
