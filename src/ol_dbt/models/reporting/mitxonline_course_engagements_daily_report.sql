with mitxonline_engagements as (
    select * from {{ ref('marts__mitxonline_course_engagements_daily') }}
)

, mitx_courses as (
    select * from {{ ref('int__mitx__courses') }}
)

select
    mitxonline_engagements.*
    , '{{ var("mitxonline") }}' as platform
    , case
        when cast(substring(mitxonline_engagements.courserun_start_on, 1, 10) as date) <= current_date
            and (
                mitxonline_engagements.courserun_end_on is null
                or cast(substring(mitxonline_engagements.courserun_end_on, 1, 10) as date) >= current_date
            )
            then true
        else false
    end as courserun_is_current
    , mitx_courses.course_readable_id
from mitxonline_engagements
left join mitx_courses
    on mitxonline_engagements.course_number = mitx_courses.course_number
