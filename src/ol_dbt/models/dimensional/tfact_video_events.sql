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

, users as (
    select * from {{ ref('dim_user') }}
)

, combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , mitxonline_video_events.openedx_user_id
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
    inner join users on mitxonline_video_events.openedx_user_id = users.mitxonline_openedx_user_id

    union all

    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , xpro_video_events.openedx_user_id
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
    inner join users on xpro_video_events.openedx_user_id = users.xpro_openedx_user_id

    union all

    select
        users.user_pk as user_fk
        , 'residential' as platform
        , mitxresidential_video_events.user_id as openedx_user_id
        , mitxresidential_video_events.courserun_readable_id
        , mitxresidential_video_events.event_type
        , mitxresidential_video_events.event_json
        , mitxresidential_video_events.video_id
        , mitxresidential_video_events.video_duration
        , mitxresidential_video_events.video_position
        , mitxresidential_video_events.starting_position
        , mitxresidential_video_events.ending_position
        , mitxresidential_video_events.event_timestamp
    from mitxresidential_video_events
    inner join users on mitxresidential_video_events.openedx_user_id = users.mitxresidential_openedx_user_id

    union all

    select
        users.user_pk as user_fk
        , 'edxorg' as platform
        , edxorg_video_events.user_id as openedx_user_id
        , edxorg_video_events.courserun_readable_id
        , edxorg_video_events.event_type
        , edxorg_video_events.event_json
        , edxorg_video_events.video_id
        , edxorg_video_events.video_duration
        , edxorg_video_events.video_position
        , edxorg_video_events.starting_position
        , edxorg_video_events.ending_position
        , edxorg_video_events.event_timestamp
    from edxorg_video_events
    inner join users on edxorg_video_events.openedx_user_id = users.edxorg_openedx_user_id
)

select distinct
    user_fk
    , platform
    , openedx_user_id
    , courserun_readable_id
    , event_type
    , video_id as video_block_fk
    , video_duration
    , video_position
    , starting_position
    , ending_position
    , event_timestamp
    , event_json

from combined
