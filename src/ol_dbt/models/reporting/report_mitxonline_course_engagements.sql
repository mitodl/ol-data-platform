with stg_edxval_coursevideo as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__edxval_coursevideo') }}
)

, mitxonline_courseactivity_video as (
    select * from {{ ref('int__mitxonline__user_courseactivity_video') }}
)

, problem_submitted as (
    select * from {{ ref('int__mitxonline__user_courseactivity_problemsubmitted') }}
)

, stg_edxval_problems as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__courseware_studentmodule') }}
)

, discussion as (
    select * from {{ ref('int__mitxonline__user_courseactivity_discussion') }}
)

, mitxonline__courserunenrollments as (
    select * from {{ ref('int__mitxonline__courserunenrollments') }}
)

, videos_played_count_tbl as (
    select
        courserun_readable_id
        , user_username
        , count(distinct useractivity_video_id) as count_videos_played
    from mitxonline_courseactivity_video
    where useractivity_event_type = 'play_video'
    group by
        courserun_readable_id
        , user_username
)

, videos_in_courserun as (
    select
        courserun_readable_id
        , count(distinct video_id) as count_videos_in_courserun
    from stg_edxval_coursevideo
    group by
        courserun_readable_id
)

, problems_submitted_count_tbl as (
    select
        courserun_readable_id
        , user_username
        , count(distinct useractivity_problem_id) as count_problems_submitted
    from problem_submitted
    group by
        courserun_readable_id
        , user_username
)

, problems_in_courserun as (
    select
        courserun_readable_id
        , count(distinct coursestructure_block_id) as count_problems_in_courserun
    from stg_edxval_problems
    where coursestructure_block_category = 'problem'
    group by
        courserun_readable_id
)

, discussions_paticipated_in_count_tbl as (
    select
        courserun_readable_id
        , user_username
        , count(distinct useractivity_discussion_block_id) as discussions_paticipated_in
    from discussion
    group by
        courserun_readable_id
        , user_username
)

, discussions_in_courserun as (
    select
        courserun_readable_id
        , count(distinct useractivity_discussion_block_id) as discussions_total
    from discussion
    group by
        courserun_readable_id
)

, courserunenrollments as (
    select
        courserun_readable_id
        , user_username
    from mitxonline__courserunenrollments
    group by
        courserun_readable_id
        , user_username
)

select
    courserunenrollments.courserun_readable_id
    , courserunenrollments.user_username
    , videos_in_courserun.count_videos_in_courserun
    , videos_played_count_tbl.count_videos_played
    , problems_submitted_count_tbl.count_problems_submitted
    , problems_in_courserun.count_problems_in_courserun
    , discussions_paticipated_in_count_tbl.discussions_paticipated_in
    , discussions_in_courserun.discussions_total
    , case 
        when videos_in_courserun.count_videos_in_courserun <> 0
            then 
                videos_played_count_tbl.count_videos_played 
                / videos_in_courserun.count_videos_in_courserun
    end
    as pct_videos_played
    , case 
        when problems_in_courserun.count_problems_in_courserun <> 0
            then 
                problems_submitted_count_tbl.count_problems_submitted 
                / problems_in_courserun.count_problems_in_courserun
    end
    as pct_problems_submitted
    , case 
        when discussions_in_courserun.discussions_total <> 0
            then 
                discussions_paticipated_in_count_tbl.discussions_paticipated_in 
                / discussions_in_courserun.discussions_total
    end
    as pct_discussions_in
from courserunenrollments
left join videos_played_count_tbl
    on
        courserunenrollments.courserun_readable_id = videos_played_count_tbl.courserun_readable_id
        and courserunenrollments.user_username = videos_played_count_tbl.user_username
left join videos_in_courserun
    on courserunenrollments.courserun_readable_id = videos_in_courserun.courserun_readable_id
left join problems_submitted_count_tbl
    on 
        courserunenrollments.courserun_readable_id = problems_submitted_count_tbl.courserun_readable_id
        and courserunenrollments.user_username = problems_submitted_count_tbl.user_username
left join problems_in_courserun
    on courserunenrollments.courserun_readable_id = problems_in_courserun.courserun_readable_id
left join discussions_paticipated_in_count_tbl
    on 
        courserunenrollments.courserun_readable_id = discussions_paticipated_in_count_tbl.courserun_readable_id
        and courserunenrollments.user_username = discussions_paticipated_in_count_tbl.user_username
left join discussions_in_courserun
    on courserunenrollments.courserun_readable_id = discussions_in_courserun.courserun_readable_id
