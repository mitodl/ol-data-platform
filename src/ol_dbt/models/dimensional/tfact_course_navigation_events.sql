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
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.old' omit quotes) as starting_tab
        , json_query(useractivity_event_object, 'lax $.new' omit quotes) as ending_tab
        , json_query(useractivity_event_object, 'lax $.current_url' omit quotes) as starting_url
        , json_query(useractivity_event_object, 'lax $.target_url' omit quotes) as ending_url
        , case
            when json_query(useractivity_event_object, 'lax $.id' omit quotes) not like 'block-v1%'
                then regexp_extract(json_query(useractivity_event_object, 'lax $.id' omit quotes), 'block-v1.*')
            else json_query(useractivity_event_object, 'lax $.id' omit quotes)
        end as block_id
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
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.old' omit quotes) as starting_tab
        , json_query(useractivity_event_object, 'lax $.new' omit quotes) as ending_tab
        , json_query(useractivity_event_object, 'lax $.current_url' omit quotes) as starting_url
        , json_query(useractivity_event_object, 'lax $.target_url' omit quotes) as ending_url
        , case
            when json_query(useractivity_event_object, 'lax $.id' omit quotes) not like 'block-v1%'
                then regexp_extract(json_query(useractivity_event_object, 'lax $.id' omit quotes), 'block-v1.*')
            else json_query(useractivity_event_object, 'lax $.id' omit quotes)
        end as block_id
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
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.old' omit quotes) as starting_tab
        , json_query(useractivity_event_object, 'lax $.new' omit quotes) as ending_tab
        , json_query(useractivity_event_object, 'lax $.current_url' omit quotes) as starting_url
        , json_query(useractivity_event_object, 'lax $.target_url' omit quotes) as ending_url
        , case
            when json_query(useractivity_event_object, 'lax $.id' omit quotes) not like 'block-v1%'
                then regexp_extract(json_query(useractivity_event_object, 'lax $.id' omit quotes), 'block-v1.*')
            else json_query(useractivity_event_object, 'lax $.id' omit quotes)
        end as block_id
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
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.old' omit quotes) as starting_tab
        , json_query(useractivity_event_object, 'lax $.new' omit quotes) as ending_tab
        , json_query(useractivity_event_object, 'lax $.current_url' omit quotes) as starting_url
        , json_query(useractivity_event_object, 'lax $.target_url' omit quotes) as ending_url
        , case
            when json_query(useractivity_event_object, 'lax $.id' omit quotes) not like 'block-v1%'
                then regexp_extract(json_query(useractivity_event_object, 'lax $.id' omit quotes), 'block-v1.*')
            else json_query(useractivity_event_object, 'lax $.id' omit quotes)
        end as block_id
        , json_query(useractivity_event_object, 'lax $.current_tab' omit quotes) as current_tab
        , json_query(useractivity_event_object, 'lax $.tab_count' omit quotes) as tab_count
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ navigation_events }}
)

, users as (
    select * from {{ ref('dim_user') }}
)

, combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , mitxonline_navigation_events.openedx_user_id
        , mitxonline_navigation_events.courserun_readable_id
        , mitxonline_navigation_events.event_type
        , mitxonline_navigation_events.event_json
        , mitxonline_navigation_events.block_id
        , coalesce(
            mitxonline_navigation_events.current_tab
            , mitxonline_navigation_events.starting_tab
            , mitxonline_navigation_events.starting_url
        ) as starting_position
        , coalesce(mitxonline_navigation_events.ending_tab, mitxonline_navigation_events.ending_url)
        as ending_position
        , mitxonline_navigation_events.current_tab
        , mitxonline_navigation_events.tab_count
        , mitxonline_navigation_events.event_timestamp
    from mitxonline_navigation_events
    inner join users on mitxonline_navigation_events.openedx_user_id = users.mitxonline_openedx_user_id


    union all

    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , xpro_navigation_events.openedx_user_id
        , xpro_navigation_events.courserun_readable_id
        , xpro_navigation_events.event_type
        , xpro_navigation_events.event_json
        , xpro_navigation_events.block_id
        , coalesce(
            xpro_navigation_events.current_tab
            , xpro_navigation_events.starting_tab
            , xpro_navigation_events.starting_url
        ) as starting_position
        , coalesce(xpro_navigation_events.ending_tab, xpro_navigation_events.ending_url) as ending_position
        , xpro_navigation_events.current_tab
        , xpro_navigation_events.tab_count
        , xpro_navigation_events.event_timestamp
    from xpro_navigation_events
    inner join users on xpro_navigation_events.openedx_user_id = users.mitxpro_openedx_user_id

    union all

    select
        users.user_pk as user_fk
        , 'residential' as platform
        , mitxresidential_navigation_events.user_id as openedx_user_id
        , mitxresidential_navigation_events.courserun_readable_id
        , mitxresidential_navigation_events.event_type
        , mitxresidential_navigation_events.event_json
        , mitxresidential_navigation_events.block_id
        , coalesce(
            mitxresidential_navigation_events.current_tab
            , mitxresidential_navigation_events.starting_tab
            , mitxresidential_navigation_events.starting_url
        ) as starting_position
        , coalesce(
            mitxresidential_navigation_events.ending_tab
            , mitxresidential_navigation_events.ending_url
        ) as ending_position
        , mitxresidential_navigation_events.current_tab
        , mitxresidential_navigation_events.tab_count
        , mitxresidential_navigation_events.event_timestamp
    from mitxresidential_navigation_events
    inner join users on mitxresidential_navigation_events.user_id = users.residential_openedx_user_id

    union all

    select
        users.user_pk as user_fk
        , 'edxorg' as platform
        , edxorg_navigation_events.user_id as openedx_user_id
        , edxorg_navigation_events.courserun_readable_id
        , edxorg_navigation_events.event_type
        , edxorg_navigation_events.event_json
        , edxorg_navigation_events.block_id
        , coalesce(
            edxorg_navigation_events.current_tab
            , edxorg_navigation_events.starting_tab
            , edxorg_navigation_events.starting_url
        ) as starting_position
        , coalesce(
            edxorg_navigation_events.ending_tab
            , edxorg_navigation_events.ending_url
        ) as ending_position
        , edxorg_navigation_events.current_tab
        , edxorg_navigation_events.tab_count
        , edxorg_navigation_events.event_timestamp
    from edxorg_navigation_events
    inner join users on edxorg_navigation_events.user_id = users.edxorg_openedx_user_id
)

select
    user_fk
    , {{ dbt_utils.generate_surrogate_key(['platform']) }} as platform_fk
    , openedx_user_id
    , courserun_readable_id
    , event_type
    , block_id as block_fk
    , starting_position
    , ending_position
    , event_timestamp
    , event_json

from combined
