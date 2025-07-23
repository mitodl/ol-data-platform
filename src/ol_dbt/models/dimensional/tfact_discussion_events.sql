{% set discussion_events = 'edx.forum.%' %}

with mitxonline_discussion_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as post_id
        , json_query(useractivity_event_object, 'lax $.title' omit quotes) as post_title
        , json_query(useractivity_event_object, 'lax $.body' omit quotes) as post_content
        , json_query(useractivity_event_object, 'lax $.commentable_id' omit quotes) as commentable_id
        , json_query(useractivity_event_object, 'lax $.category_id' omit quotes) as discussion_component_id
        , json_query(useractivity_event_object, 'lax $.category_name' omit quotes) as discussion_component_name
        , json_query(useractivity_event_object, 'lax $.url' omit quotes) as page_url
        , json_query(useractivity_event_object, 'lax $.user_forums_roles' omit quotes) as user_forums_roles
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type like '{{ discussion_events }}'
)

, xpro_discussion_events as (

    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as post_id
        , json_query(useractivity_event_object, 'lax $.title' omit quotes) as post_title
        , json_query(useractivity_event_object, 'lax $.body' omit quotes) as post_content
        , json_query(useractivity_event_object, 'lax $.commentable_id' omit quotes) as commentable_id
        , json_query(useractivity_event_object, 'lax $.category_id' omit quotes) as discussion_component_id
        , json_query(useractivity_event_object, 'lax $.category_name' omit quotes) as discussion_component_name
        , json_query(useractivity_event_object, 'lax $.url' omit quotes) as page_url
        , json_query(useractivity_event_object, 'lax $.user_forums_roles' omit quotes) as user_forums_roles
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type like '{{ discussion_events }}'
)

, mitxresidential_discussion_events as (

    select
        user_username
        , user_id as openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as post_id
        , json_query(useractivity_event_object, 'lax $.title' omit quotes) as post_title
        , json_query(useractivity_event_object, 'lax $.body' omit quotes) as post_content
        , json_query(useractivity_event_object, 'lax $.commentable_id' omit quotes) as commentable_id
        , json_query(useractivity_event_object, 'lax $.category_id' omit quotes) as discussion_component_id
        , json_query(useractivity_event_object, 'lax $.category_name' omit quotes) as discussion_component_name
        , json_query(useractivity_event_object, 'lax $.url' omit quotes) as page_url
        , json_query(useractivity_event_object, 'lax $.user_forums_roles' omit quotes) as user_forums_roles
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type like '{{ discussion_events }}'
)

, edxorg_discussion_events as (
    select
        user_username
        , user_id as openedx_user_id
        , {{ format_course_id('courserun_readable_id') }} as courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_event_object, 'lax $.id' omit quotes) as post_id
        , json_query(useractivity_event_object, 'lax $.title' omit quotes) as post_title
        , json_query(useractivity_event_object, 'lax $.body' omit quotes) as post_content
        , json_query(useractivity_event_object, 'lax $.commentable_id' omit quotes) as commentable_id
        , json_query(useractivity_event_object, 'lax $.category_id' omit quotes) as discussion_component_id
        , json_query(useractivity_event_object, 'lax $.category_name' omit quotes) as discussion_component_name
        , json_query(useractivity_event_object, 'lax $.url' omit quotes) as page_url
        , json_query(useractivity_event_object, 'lax $.user_forums_roles' omit quotes) as user_forums_roles
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type like '{{ discussion_events }}'
)

, users as (
    select * from {{ ref('dim_user') }}
)

, combined as (
    select
        'mitxonline' as platform
        , mitxonline_discussion_events.openedx_user_id
        , mitxonline_discussion_events.user_username
        , users.user_pk as user_fk
        , mitxonline_discussion_events.courserun_readable_id
        , mitxonline_discussion_events.event_type
        , mitxonline_discussion_events.event_json
        , mitxonline_discussion_events.post_id
        , mitxonline_discussion_events.post_title
        , mitxonline_discussion_events.post_content
        , mitxonline_discussion_events.commentable_id
        , mitxonline_discussion_events.discussion_component_id
        , mitxonline_discussion_events.discussion_component_name
        , mitxonline_discussion_events.page_url
        , mitxonline_discussion_events.user_forums_roles
        , mitxonline_discussion_events.event_timestamp
    from mitxonline_discussion_events
    left join users
        on
            mitxonline_discussion_events.openedx_user_id = users.mitxonline_openedx_user_id
            and mitxonline_discussion_events.user_username = users.user_mitxonline_username

    union all

    select
        'mitxpro' as platform
        , xpro_discussion_events.openedx_user_id
        , xpro_discussion_events.user_username
        , users.user_pk as user_fk
        , xpro_discussion_events.courserun_readable_id
        , xpro_discussion_events.event_type
        , xpro_discussion_events.event_json
        , xpro_discussion_events.post_id
        , xpro_discussion_events.post_title
        , xpro_discussion_events.post_content
        , xpro_discussion_events.commentable_id
        , xpro_discussion_events.discussion_component_id
        , xpro_discussion_events.discussion_component_name
        , xpro_discussion_events.page_url
        , xpro_discussion_events.user_forums_roles
        , xpro_discussion_events.event_timestamp
    from xpro_discussion_events
    left join users
        on
            xpro_discussion_events.openedx_user_id = users.mitxonline_openedx_user_id
            and xpro_discussion_events.user_username = users.user_mitxonline_username

    union all

    select
        'residential' as platform
        , mitxresidential_discussion_events.openedx_user_id
        , mitxresidential_discussion_events.user_username
        , users.user_pk as user_fk
        , mitxresidential_discussion_events.courserun_readable_id
        , mitxresidential_discussion_events.event_type
        , mitxresidential_discussion_events.event_json
        , mitxresidential_discussion_events.post_id
        , mitxresidential_discussion_events.post_title
        , mitxresidential_discussion_events.post_content
        , mitxresidential_discussion_events.commentable_id
        , mitxresidential_discussion_events.discussion_component_id
        , mitxresidential_discussion_events.discussion_component_name
        , mitxresidential_discussion_events.page_url
        , mitxresidential_discussion_events.user_forums_roles
        , mitxresidential_discussion_events.event_timestamp
    from mitxresidential_discussion_events
    left join users
        on
            mitxresidential_discussion_events.openedx_user_id = users.residential_openedx_user_id
            and mitxresidential_discussion_events.user_username = users.user_residential_username

    union all

    select
        'edxorg' as platform
        , edxorg_discussion_events.openedx_user_id
        , edxorg_discussion_events.user_username
        , users.user_pk as user_fk
        , edxorg_discussion_events.courserun_readable_id
        , edxorg_discussion_events.event_type
        , edxorg_discussion_events.event_json
        , edxorg_discussion_events.post_id
        , edxorg_discussion_events.post_title
        , edxorg_discussion_events.post_content
        , edxorg_discussion_events.commentable_id
        , edxorg_discussion_events.discussion_component_id
        , edxorg_discussion_events.discussion_component_name
        , edxorg_discussion_events.page_url
        , edxorg_discussion_events.user_forums_roles
        , edxorg_discussion_events.event_timestamp
    from edxorg_discussion_events
    left join users
        on
            edxorg_discussion_events.openedx_user_id = users.edxorg_openedx_user_id
            and edxorg_discussion_events.user_username = users.user_edxorg_username
)

select
    {{ dbt_utils.generate_surrogate_key(['platform']) }} as platform_fk
    , platform
    , user_fk
    , openedx_user_id
    , user_username
    , courserun_readable_id
    , event_type
    , event_json
    , post_id
    , post_title
    , post_content
    , commentable_id
    , discussion_component_id
    , discussion_component_name
    , page_url
    , user_forums_roles
    , event_timestamp
from combined
