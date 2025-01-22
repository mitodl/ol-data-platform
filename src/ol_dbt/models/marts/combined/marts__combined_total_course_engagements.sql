with combined_problems as (
    select 
        '{{ var("mitxonline") }}' as platform
        , courserun_readable_id
        , count(distinct coursestructure_block_id) as problem_count
    from {{ ref('stg__mitxonline__openedx__api__course_structure') }}
    where coursestructure_block_category = 'problem'
    group by courserun_readable_id

    union all

    select 
        '{{ var("edxorg") }}' as platform
        , courserun_readable_id
        , count(distinct coursestructure_block_id) as problem_count
    from {{ ref('stg__edxorg__s3__course_structure') }}
    where coursestructure_block_category = 'problem'
    group by courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , courserun_readable_id
        , count(distinct coursestructure_block_id) as problem_count
    from {{ ref('stg__mitxpro__openedx__api__course_structure') }}
    where coursestructure_block_category = 'problem'
    group by courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform
        , courserun_readable_id
        , count(distinct coursestructure_block_id) as problem_count
    from {{ ref('stg__mitxresidential__openedx__api__course_structure') }}
    where coursestructure_block_category = 'problem'
    group by courserun_readable_id
)

, combined_videos as (
    select
        '{{ var("mitxonline") }}' as platform
        , courserun_readable_id
        , count(distinct video_id) as total_course_videos
    from {{ ref('stg__mitxonline__openedx__mysql__edxval_coursevideo') }}
    where coursevideo_is_hidden = false
    group by courserun_readable_id

    union all

    select 
        '{{ var("edxorg") }}' as platform
        , courserun_readable_id
        , count(distinct video_block_id) as total_course_videos
    from {{ ref('stg__edxorg__s3__course_video') }}
    group by courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , courserun_readable_id
        , count(distinct video_id) as total_course_videos
    from {{ ref('stg__mitxpro__openedx__mysql__edxval_coursevideo') }}
    where coursevideo_is_hidden = false
    group by courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform
        , courserun_readable_id
        , count(distinct video_id) as total_course_videos
    from {{ ref('stg__mitxresidential__openedx__edxval_coursevideo') }}
    where coursevideo_is_hidden = false
    group by courserun_readable_id
)


, combined_discussion as (
    select
        '{{ var("mitxonline") }}' as platform
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as total_discussions
    from {{ ref('int__mitxonline__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by courserun_readable_id

    union all

    select
        '{{ var("edxorg") }}' as platform
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as total_discussions
    from {{ ref('int__edxorg__mitx_user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as total_discussions
    from {{ ref('int__mitxpro__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform 
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as total_discussions
    from {{ ref('int__mitxresidential__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by courserun_readable_id
)

, combined_runs as (
    select * 
    from {{ ref('int__combined__course_runs') }}
    where courserun_readable_id is not null
)

select
    combined_runs.platform
    , combined_runs.courserun_readable_id
    , combined_problems.problem_count as total_courserun_problems
    , combined_videos.total_course_videos as total_courserun_videos
    , combined_discussion.total_discussions as total_courserun_discussions
    , combined_runs.courserun_title
    , combined_runs.course_readable_id
    , combined_runs.courserun_is_current
    , combined_runs.courserun_start_on
    , combined_runs.courserun_end_on
from combined_runs
left join combined_problems
    on combined_runs.courserun_readable_id = combined_problems.courserun_readable_id
left join combined_videos
    on combined_runs.courserun_readable_id = combined_videos.courserun_readable_id
left join combined_discussion
    on combined_runs.courserun_readable_id = combined_discussion.courserun_readable_id