{% set video_events =
    (
    'load_video'
    , 'play_video'
    , 'seek_video'
    , 'pause_video'
    , 'stop_video'
    , 'complete_video'
    , 'speed_change_video'
    , 'show_transcript'
    , 'hide_transcript'
    , 'edx.video.closed_captions.hidden'
    , 'edx.video.closed_captions.shown'
    )
%}

with mitxonline_video_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as video_id
        , case
            when lower(json_query(useractivity_event_object, 'lax $.duration' omit quotes)) = 'null' then null
            else cast(json_query(useractivity_event_object, 'lax $.duration' omit quotes) as decimal(38, 4))
        end as video_duration
        , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as video_position
        , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as starting_position
        , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as ending_position
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ video_events }}
)

, xpro_video_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as video_id
        , case
            when lower(json_query(useractivity_event_object, 'lax $.duration' omit quotes)) = 'null' then null
            else cast(json_query(useractivity_event_object, 'lax $.duration' omit quotes) as decimal(38, 4))
        end as video_duration
        , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as video_position
        , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as starting_position
        , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as ending_position
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ video_events }}
)

, mitxresidential_video_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as video_id
        , case
            when lower(json_query(useractivity_event_object, 'lax $.duration' omit quotes)) = 'null' then null
            else cast(json_query(useractivity_event_object, 'lax $.duration' omit quotes) as decimal(38, 4))
        end as video_duration
        , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as video_position
        , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as starting_position
        , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as ending_position
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ video_events }}
)

, edxorg_video_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as video_id
        , case
            when lower(json_query(useractivity_event_object, 'lax $.duration' omit quotes)) = 'null' then null
            else cast(json_query(useractivity_event_object, 'lax $.duration' omit quotes) as decimal(38, 4))
        end as video_duration
        , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as video_position
        , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as starting_position
        , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as ending_position
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ video_events }}
)

, mitxonline_users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, xpro_users as (
    select * from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)


, combined as (
    select
        'mitxonline' as platform
        , coalesce(mitxonline_users.user_id, mitxonline_video_events.openedx_user_id) as user_id
        , mitxonline_video_events.courserun_readable_id
        , mitxonline_video_events.event_type
        , mitxonline_video_events.event_json
        , mitxonline_video_events.video_id
        , mitxonline_video_events.video_duration
        , mitxonline_video_events.video_position
        , mitxonline_video_events.starting_position
        , mitxonline_video_events.ending_position
        , mitxonline_video_events.event_timestamp
    from mitxonline_video_events
    left join mitxonline_users on mitxonline_video_events.user_username = mitxonline_users.user_username

    union all

    select
        'mitxpro' as platform
        , coalesce(xpro_users.user_id, xpro_video_events.openedx_user_id) as user_id
        , xpro_video_events.courserun_readable_id
        , xpro_video_events.event_type
        , xpro_video_events.event_json
        , xpro_video_events.video_id
        , xpro_video_events.video_duration
        , xpro_video_events.video_position
        , xpro_video_events.starting_position
        , xpro_video_events.ending_position
        , xpro_video_events.event_timestamp
    from xpro_video_events
    left join xpro_users on xpro_video_events.user_username = xpro_users.user_username

    union all

    select
        'residential' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , event_json
        , video_id
        , video_duration
        , video_position
        , starting_position
        , ending_position
        , event_timestamp
    from mitxresidential_video_events

    union all

    select
        'edxorg' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , event_json
        , video_id
        , video_duration
        , video_position
        , starting_position
        , ending_position
        , event_timestamp
    from edxorg_video_events
)

select distinct
     {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , platform as platform_id
    , courserun_readable_id
    , event_type
    , video_id
    , video_duration
    , video_position
    , starting_position
    , ending_position
    , event_timestamp
    , event_json

from combined
