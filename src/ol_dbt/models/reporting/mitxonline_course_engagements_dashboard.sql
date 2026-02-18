with base as (
    select * from {{ ref('marts__mitxonline_course_engagements_daily') }}
)

, courses as (
    select * from {{ ref('int__mitx__courses') }}
)

select
    base.courseactivity_date
    , base.user_username
    , base.user_full_name
    , base.user_email
    , base.courserun_readable_id
    , base.courserun_title
    , base.course_number
    , base.courserun_start_on
    , base.courserun_end_on
    , base.num_events
    , base.num_problem_submitted
    , base.num_video_played
    , base.num_discussion_participated
    , '{{ var("mitxonline") }}' as platform
    , case
        when cast(substring(base.courserun_start_on, 1, 10) as date) <= current_date
            and (
                base.courserun_end_on is null
                or cast(substring(base.courserun_end_on, 1, 10) as date) >= current_date
            )
        then true
        else false
    end as courserun_is_current
    , courses.course_readable_id
from base
left join courses
    on base.course_number = courses.course_number
