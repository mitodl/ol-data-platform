with ordered_video_events as (
    select
        user_id
        , courserun_readable_id
        , video_id
        , event_type
  		, video_position -- where the event occurred
  		, starting_position -- where the user was before seeking
  		, ending_position -- where the user was after seeking
  		, video_duration
  		, event_timestamp
        , event_json
        , case
            when event_type = 'complete_video' then 1
            else 0
        end as video_completion
        -- Utilize value navigation window functions to assess the sequence of events
        , lag(event_type) over (
            partition by user_id, video_id
            order by event_timestamp
        ) as previous_event
        , lead(event_type) over (
            partition by user_id, video_id
            order by event_timestamp
        ) as next_event
        , first_value(event_type) over (
            partition by user_id, video_id
            order by event_timestamp
        ) as first_event
        , last_value(event_type) over (
            partition by user_id, video_id
            order by event_timestamp
            rows between unbounded preceeding and unbounded following
        ) as last_event
        , nth_value(event_type, 2) over (
            partition by user_id, video_id
            order by event_timestamp
            rows between unbounded preceeding and unbounded following
        ) as second_event
    from tfact_video_events
    order by event_timestamp
)

, video_watched as (
    select
        user_id
        , video_id
        , cast(subsequent_event.video_position as double) - cast(initial_event.video_position as double) as watched_time
    from ordered_video_events as initial_event
    join ordered_video_events as subsequent_event
        on initial_event.user_id = subsequent_event.user_id
        and initial_event.video_id = subsequent_event.video_id
        -- todo: how to make sure I am getting the correct subsequent event?
    where initial_event.event_type = 'play_video'
        and subsequent_event.event_type in ('pause_video', 'stop_video', 'complete_video')
    order by event_timestamp
)

, video_skipped as (
    select
        user_id
        , video_id
        -- positive skipped_time means they replayed content they already watched
        -- negative skipped_time here means they skipped to end
        , (cast(starting_position as double)-cast(ending_position as double)) as skipped_time
    from ordered_video_events
    where event_type = 'seek_video'
)

, engagement_events as (
    select
        user_id
        , video_id
        , sum(case
            -- increased engagement indicators
                -- replay video
                when event_type = 'seek_video' and (starting_position > ending_position) then 1
                -- enable CC
                when event_type = 'edx.video.closed_captions.shown' then 1
                -- enable transcript
                when event_type = 'show_transcript' then 1
                -- slow down video when replaying
                when event_type = 'speed_change_video' and (starting_position > ending_position) then 1
            -- decreased engagement indicators
                -- skip video
                when event_type = 'seek_video' and (ending_position > starting_position) then -1
                -- disable CC
                when event_type = 'edx.video.closed_captions.hidden' then -1
                -- disable transcript
                when event_type = 'hide_transcript' then -1
                -- speeds up video (especially while skipping)
                when event_type = 'speed_change_video' and (ending_position > starting_position) then -1
        end) as engagement_score
        , count(*) as event_count
    from tfact_video_events
    group by user_id, video_id
)

, course_sections as (
    select
        courserun_readable_id
        , parent_block_id as section_block_id
        , parent.block_title as section_name
        , block_id as subsection_block_id
        , block_title as subsection_name
    from dim_course_content
    left join dim_course_content as parent
        on dim_course_content.parent_block_id = parent.block_id
    where block_category = 'video'
)

, combined as (
    select
        tfact_video_events.user_id
        , tfact_video_events.courserun_readable_id
        , tfact_video_events.video_id
        , course_sections.block_title as video_name
        , tfact_video_events.video_duration
        -- subtract skipped time, add rewatched time
        , video_watched.watched_time + video_skipped.skipped_time as watched_time
        , tfact_video_events.video_completion
        , engagement_events.engagement_score / engagement_events.event_count as video_engagement
    from tfact_video_events
    left join video_watched
        on tfact_video_events.user_id = video_watched.user_id
        and tfact_video_events.video_id = video_watched.video_id
    left join video_skipped
        on tfact_video_events.user_id = video_skipped.user_id
        and tfact_video_events.video_id = video_skipped.video_id
    left join engagement_events
        on tfact_video_events.user_id = engagement_events.user_id
        and tfact_video_events.video_id = engagement_events.video_id
    left join course_sections
        on video_watched.video_id = course_sections.subsection_block_id
)

select distinct
    user_id
    , courserun_readable_id
    , video_id
    , video_name
    , video_duration
    , watched_time
    , video_completion
    , video_engagement
from combined
