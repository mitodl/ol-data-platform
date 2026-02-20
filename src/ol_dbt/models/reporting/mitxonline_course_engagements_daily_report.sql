with mitxonline_engagements as (
    select * from {{ ref('marts__mitxonline_course_engagements_daily') }}
)

, mitxonline_course_runs as (
    select
        courserun_readable_id
        , course_id
    from {{ ref('int__mitxonline__course_runs') }}
)

, mitx_courses as (
    select
        mitxonline_course_id
        , course_readable_id
    from {{ ref('int__mitx__courses') }}
)

select
    mitxonline_engagements.*
    , '{{ var("mitxonline") }}' as platform
    , case
        when
            mitxonline_engagements.courserun_end_on is null
            and from_iso8601_timestamp(mitxonline_engagements.courserun_start_on) <= current_date
            then true
        when
            from_iso8601_timestamp(mitxonline_engagements.courserun_start_on) <= current_date
            and from_iso8601_timestamp(mitxonline_engagements.courserun_end_on) > current_date
            then true
        else false
    end as courserun_is_current
    , mitx_courses.course_readable_id
from mitxonline_engagements
inner join mitxonline_course_runs
    on mitxonline_engagements.courserun_readable_id = mitxonline_course_runs.courserun_readable_id
left join mitx_courses
    on mitxonline_course_runs.course_id = mitx_courses.mitxonline_course_id
