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

select
    combined_runs.platform
    , combined_runs.courserun_readable_id
    , combined_engagements.total_courserun_problems
    , combined_engagements.total_courserun_videos
    , combined_engagements.total_courserun_discussions
    , combined_runs.courserun_title
    , combined_runs.course_readable_id
    , combined_runs.courserun_is_current
    , combined_runs.courserun_start_on
    , combined_runs.courserun_end_on
from combined_runs
inner join combined_engagements
    on 
        combined_runs.courserun_readable_id = combined_engagements.courserun_readable_id
        and combined_runs.platform = combined_engagements.platform