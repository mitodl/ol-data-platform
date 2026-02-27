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
    mitxonline_engagements.courseactivity_date
    , mitxonline_engagements.user_username
    , mitxonline_engagements.user_full_name
    , mitxonline_engagements.user_email
    , mitxonline_engagements.courserun_readable_id
    , mitxonline_engagements.courserun_title
    , mitxonline_engagements.course_number
    , mitxonline_engagements.courserun_start_on
    , mitxonline_engagements.courserun_end_on
    , mitxonline_engagements.num_events
    , mitxonline_engagements.num_problem_submitted
    , mitxonline_engagements.num_video_played
    , mitxonline_engagements.num_discussion_participated
    , '{{ var("mitxonline") }}' as platform
    , {{ is_courserun_current('mitxonline_engagements.courserun_start_on', 'mitxonline_engagements.courserun_end_on') }} as courserun_is_current
    , mitx_courses.course_readable_id
from mitxonline_engagements
inner join mitxonline_course_runs
    on mitxonline_engagements.courserun_readable_id = mitxonline_course_runs.courserun_readable_id
left join mitx_courses
    on mitxonline_course_runs.course_id = mitx_courses.mitxonline_course_id
