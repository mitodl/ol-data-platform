{% set navigation_events =
    (
    'edx.ui.lms.jump_nav.selected'
    , 'edx.ui.lms.link_clicked'
    , 'edx.ui.lms.outline.selected'
    , 'edx.ui.lms.sequence.next_selected'
    , 'edx.ui.lms.sequence.previous_selected'
    , 'edx.ui.lms.sequence.tab_selected'
    )
%}

with mitxonline_navigation_events as (
    select
        user_username
        , courserun_readable_id
        , useractivity_event_type as event_type
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as vertical_block_id
        , json_query(useractivity_event_object, 'lax $.current_tab' omit quotes) as current_tab
        , json_query(useractivity_event_object, 'lax $.tab_count' omit quotes) as tab_count
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ navigation_events }}
)

, xpro_navigation_events as (
    select
        user_username
        , courserun_readable_id
        , useractivity_event_type as event_type
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as vertical_block_id
        , json_query(useractivity_event_object, 'lax $.current_tab' omit quotes) as current_tab
        , json_query(useractivity_event_object, 'lax $.tab_count' omit quotes) as tab_count
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ navigation_events }}
)

, mitxresidential_navigation_events as (
    select
        user_username
        , courserun_readable_id
        , useractivity_event_type as event_type
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as vertical_block_id
        , json_query(useractivity_event_object, 'lax $.current_tab' omit quotes) as current_tab
        , json_query(useractivity_event_object, 'lax $.tab_count' omit quotes) as tab_count
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ navigation_events }}
)

, edxorg_navigation_events as (
    select
        user_username
        , courserun_readable_id
        , useractivity_event_type as event_type
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as vertical_block_id
        , json_query(useractivity_event_object, 'lax $.current_tab' omit quotes) as current_tab
        , json_query(useractivity_event_object, 'lax $.tab_count' omit quotes) as tab_count
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ navigation_events }}
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
        , mitxonline_users.user_id
        , mitxonline_navigation_events.courserun_readable_id
        , mitxonline_navigation_events.event_type
        , mitxonline_navigation_events.vertical_block_id
        , mitxonline_navigation_events.current_tab
        , mitxonline_navigation_events.tab_count
        , mitxonline_navigation_events.event_timestamp
    from mitxonline_navigation_events
    left join mitxonline_users on mitxonline_navigation_events.user_username = mitxonline_users.user_username

    union all

    select
        'mitxpro' as platform
        , xpro_users.user_id
        , xpro_navigation_events.courserun_readable_id
        , xpro_navigation_events.event_type
        , xpro_navigation_events.vertical_block_id
        , xpro_navigation_events.current_tab
        , xpro_navigation_events.tab_count
        , xpro_navigation_events.event_timestamp
    from xpro_navigation_events
    left join xpro_users on xpro_navigation_events.user_username = xpro_users.user_username

    union all

    select
        'residential' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , vertical_block_id
        , current_tab
        , tab_count
        , event_timestamp
    from mitxresidential_navigation_events

    union all

    select
        'edxorg' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , vertical_block_id
        , current_tab
        , tab_count
        , event_timestamp
    from edxorg_navigation_events
)

select
     {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , platform as platform_id
    , courserun_readable_id
    , event_type
    , vertical_block_id
    , event_timestamp

from combined
