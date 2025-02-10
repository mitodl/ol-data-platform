with combined_engagements as (
    select
        platform
        , courserun_readable_id
        , count(
            distinct
            case
                when coursestructure_block_category = 'discussion'
                    then coursestructure_block_id
            end
        ) as total_courserun_discussions
        , count(
            distinct
            case
                when coursestructure_block_category = 'video'
                    then coursestructure_block_id
            end
        ) as total_courserun_videos
        , count(
            distinct
            case
                when coursestructure_block_category = 'problem'
                    then coursestructure_block_id
            end
        ) as total_courserun_problems
    from {{ ref('int__combined__course_structure') }}
    group by
        platform
        , courserun_readable_id
)

, combined_runs as (
    select *
    from {{ ref('int__combined__course_runs') }}
    where courserun_readable_id is not null
)

, combined_enrollments as (
    select
        platform
        , courserun_readable_id
        , user_username
    from {{ ref('int__combined__courserun_enrollments') }}
    where courserun_readable_id is not null
    group by
        platform
        , courserun_readable_id
        , user_username
)

, combined_user_video as (

    select
        '{{ var("mitxonline") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_video_id) as videos_user_watched
    from {{ ref('int__mitxonline__user_courseactivity_video') }}
    where useractivity_event_type = 'play_video'
    group by
        '{{ var("mitxonline") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_video_id) as videos_user_watched
    from {{ ref('int__edxorg__mitx_user_courseactivity_video') }}
    where useractivity_event_type = 'play_video'
    group by
        '{{ var("edxorg") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_video_id) as videos_user_watched
    from {{ ref('int__mitxpro__user_courseactivity_video') }}
    where useractivity_event_type = 'play_video'
    group by
        '{{ var("mitxpro") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_video_id) as videos_user_watched
    from {{ ref('int__mitxresidential__user_courseactivity_video') }}
    where useractivity_event_type = 'play_video'
    group by
        '{{ var("residential") }}'
        , user_username
        , courserun_readable_id
)

, combined_user_problem as (

    select
        '{{ var("mitxonline") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_problem_id) as problems_user_submitted
    from {{ ref('int__mitxonline__user_courseactivity_problemsubmitted') }}
    group by
        '{{ var("mitxonline") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_problem_id) as problems_user_submitted
    from {{ ref('int__edxorg__mitx_user_courseactivity_problemsubmitted') }}
    group by
        '{{ var("edxorg") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_problem_id) as problems_user_submitted
    from {{ ref('int__mitxpro__user_courseactivity_problemsubmitted') }}
    group by
        '{{ var("mitxpro") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_problem_id) as problems_user_submitted
    from {{ ref('int__mitxresidential__user_courseactivity_problemsubmitted') }}
    group by
        '{{ var("residential") }}'
        , user_username
        , courserun_readable_id
)

, combined_user_discussion as (

    select
        '{{ var("mitxonline") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as user_discussion_count
    from {{ ref('int__mitxonline__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by
        '{{ var("mitxonline") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as user_discussion_count
    from {{ ref('int__edxorg__mitx_user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by
        '{{ var("edxorg") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as user_discussion_count
    from {{ ref('int__mitxpro__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by
        '{{ var("mitxpro") }}'
        , user_username
        , courserun_readable_id

    union all

    select
        '{{ var("residential") }}' as platform
        , user_username
        , courserun_readable_id
        , count(distinct useractivity_discussion_post_id) as user_discussion_count
    from {{ ref('int__mitxresidential__user_courseactivity_discussion') }}
    where useractivity_event_type like 'edx.forum.%.created'
    group by
        '{{ var("residential") }}'
        , user_username
        , courserun_readable_id
)

, user_video_correction as (
    select
        platform
        , courserun_readable_id
        , max(videos_user_watched) as max_videos_per_courserun
    from combined_user_video
    group by
        platform
        , courserun_readable_id
)

, user_problem_correction as (
    select
        platform
        , courserun_readable_id
        , max(problems_user_submitted) as max_problems_per_courserun
    from combined_user_problem
    group by
        platform
        , courserun_readable_id
)

, user_discussion_correction as (
    select
        platform
        , courserun_readable_id
        , max(user_discussion_count) as max_discussions_per_courserun
    from combined_user_discussion
    group by
        platform
        , courserun_readable_id
)

select
    combined_runs.platform
    , combined_runs.courserun_readable_id
    , combined_runs.courserun_title
    , combined_runs.course_readable_id
    , combined_runs.courserun_is_current
    , combined_runs.courserun_start_on
    , combined_runs.courserun_end_on
    , combined_enrollments.user_username
    , combined_user_video.videos_user_watched
    , combined_user_problem.problems_user_submitted
    , combined_user_discussion.user_discussion_count
    , case
        when combined_engagements.total_courserun_problems >= user_problem_correction.max_problems_per_courserun
            then combined_engagements.total_courserun_problems
        else user_problem_correction.max_problems_per_courserun
    end as total_courserun_problems
    , case
        when combined_engagements.total_courserun_videos >= user_video_correction.max_videos_per_courserun
            then combined_engagements.total_courserun_videos
        else user_video_correction.max_videos_per_courserun
    end as total_courserun_videos
    , case
        when
            combined_engagements.total_courserun_discussions
            >= user_discussion_correction.max_discussions_per_courserun
            then combined_engagements.total_courserun_discussions
        else user_discussion_correction.max_discussions_per_courserun
    end as total_courserun_discussions
from combined_runs
inner join combined_engagements
    on
        combined_runs.courserun_readable_id = combined_engagements.courserun_readable_id
        and combined_runs.platform = combined_engagements.platform
inner join combined_enrollments
    on
        combined_runs.courserun_readable_id = combined_enrollments.courserun_readable_id
        and combined_runs.platform = combined_enrollments.platform
left join combined_user_video
    on
        combined_enrollments.courserun_readable_id = combined_user_video.courserun_readable_id
        and combined_enrollments.platform = combined_user_video.platform
        and combined_enrollments.user_username = combined_user_video.user_username
left join combined_user_problem
    on
        combined_enrollments.courserun_readable_id = combined_user_problem.courserun_readable_id
        and combined_enrollments.platform = combined_user_problem.platform
        and combined_enrollments.user_username = combined_user_problem.user_username
left join combined_user_discussion
    on
        combined_enrollments.courserun_readable_id = combined_user_discussion.courserun_readable_id
        and combined_enrollments.platform = combined_user_discussion.platform
        and combined_enrollments.user_username = combined_user_discussion.user_username
left join user_video_correction
    on
        combined_runs.courserun_readable_id = user_video_correction.courserun_readable_id
        and combined_runs.platform = user_video_correction.platform
left join user_problem_correction
    on
        combined_runs.courserun_readable_id = user_problem_correction.courserun_readable_id
        and combined_runs.platform = user_problem_correction.platform
left join user_discussion_correction
    on
        combined_runs.courserun_readable_id = user_discussion_correction.courserun_readable_id
        and combined_runs.platform = user_discussion_correction.platform
