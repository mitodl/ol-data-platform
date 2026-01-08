{{ config(full_refresh = true) }}

with mitxonline_video_engagements as (
    select * from {{ ref('marts__mitxonline_video_engagements') }}
)

, mitx__courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, subq_video_engagements as (
    select
        section_title
        , courserun_readable_id
        , count(distinct video_title) as total_video_count
    from mitxonline_video_engagements
    group by
        section_title
        , courserun_readable_id
)

select
    mitxonline_video_engagements.user_email
    , mitxonline_video_engagements.section_title
    , mitxonline_video_engagements.courserun_readable_id
    , subq_video_engagements.total_video_count
    , 'MITx Online' as platform
    , case
        when
                cast(substring(mitxonline_video_engagements.courserun_start_on, 1, 10) as date) <= current_date
            and
                (mitxonline_video_engagements.courserun_end_on is null
                or cast(substring(mitxonline_video_engagements.courserun_end_on, 1, 10) as date) >= current_date)
        then true
        else false end
    as courserun_is_current
    , mitx__courses.course_readable_id
    , count(distinct mitxonline_video_engagements.video_title) as user_watched_video_count
    , max(mitxonline_video_engagements.coursestructure_block_index) as max_coursestructure_block_index
from mitxonline_video_engagements
join subq_video_engagements
    on
        mitxonline_video_engagements.section_title = subq_video_engagements.section_title
        and mitxonline_video_engagements.courserun_readable_id = subq_video_engagements.courserun_readable_id
left join mitx__courses
    on mitxonline_video_engagements.course_number = mitx__courses.course_number
where mitxonline_video_engagements.video_event_type = 'play_video'
group by
    mitxonline_video_engagements.user_email
    , mitxonline_video_engagements.section_title
    , mitxonline_video_engagements.courserun_readable_id
    , subq_video_engagements.total_video_count
    , 'MITx Online'
    , case
        when
                cast(substring(mitxonline_video_engagements.courserun_start_on, 1, 10) as date) <= current_date
            and
                (mitxonline_video_engagements.courserun_end_on is null
                or cast(substring(mitxonline_video_engagements.courserun_end_on, 1, 10) as date) >= current_date)
        then true
        else false end
    , mitx__courses.course_readable_id
