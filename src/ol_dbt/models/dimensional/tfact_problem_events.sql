{% set problem_events =
    (
    'problem_check'
    , 'showanswer'
    )
%}

with mitxonline_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id
        , json_query(useractivity_event_object, 'lax $.answers' omit quotes) as answers
        , json_query(useractivity_event_object, 'lax $.attempts' omit quotes) as attempt
        , json_query(useractivity_event_object, 'lax $.success' omit quotes) as success
        , json_query(useractivity_event_object, 'lax $.grade' omit quotes) as grade
        , json_query(useractivity_event_object, 'lax $.max_grade' omit quotes) as max_grade
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, xpro_problem_events as (
    select
        user_username
        , openedx_user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id
        , json_query(useractivity_event_object, 'lax $.answers' omit quotes) as answers
        , json_query(useractivity_event_object, 'lax $.attempts' omit quotes) as attempt
        , json_query(useractivity_event_object, 'lax $.success' omit quotes) as success
        , json_query(useractivity_event_object, 'lax $.grade' omit quotes) as grade
        , json_query(useractivity_event_object, 'lax $.max_grade' omit quotes) as max_grade
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, mitxresidential_problem_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id
        , json_query(useractivity_event_object, 'lax $.answers' omit quotes) as answers
        , json_query(useractivity_event_object, 'lax $.attempts' omit quotes) as attempt
        , json_query(useractivity_event_object, 'lax $.success' omit quotes) as success
        , json_query(useractivity_event_object, 'lax $.grade' omit quotes) as grade
        , json_query(useractivity_event_object, 'lax $.max_grade' omit quotes) as max_grade
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, edxorg_problem_events as (
    select
        user_username
        , user_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id
        , json_query(useractivity_event_object, 'lax $.answers' omit quotes) as answers
        , json_query(useractivity_event_object, 'lax $.attempts' omit quotes) as attempt
        , json_query(useractivity_event_object, 'lax $.success' omit quotes) as success
        , json_query(useractivity_event_object, 'lax $.grade' omit quotes) as grade
        , json_query(useractivity_event_object, 'lax $.max_grade' omit quotes) as max_grade
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ problem_events }}
        and useractivity_event_source = 'server'
)

, users as (
    select
        user_pk
        , mitxonline_openedx_user_id
        , mitxpro_openedx_user_id
        , residential_openedx_user_id
        , edxorg_openedx_user_id
    from {{ ref('dim_user') }}
)

, combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , mitxonline_problem_events.openedx_user_id
        , mitxonline_problem_events.courserun_readable_id
        , mitxonline_problem_events.event_type
        , mitxonline_problem_events.event_json
        , mitxonline_problem_events.problem_block_id
        , mitxonline_problem_events.answers
        , mitxonline_problem_events.attempt
        , mitxonline_problem_events.success
        , mitxonline_problem_events.grade
        , mitxonline_problem_events.max_grade
        , mitxonline_problem_events.event_timestamp
    from mitxonline_problem_events
    inner join users
        on
            mitxonline_problem_events.openedx_user_id = users.mitxonline_openedx_user_id
            and mitxonline_problem_events.user_username = users.user_mitxonline_username

    union all

    select
        '' as user_fk
        , 'mitxpro' as platform
        , xpro_problem_events.openedx_user_id
        , xpro_problem_events.courserun_readable_id
        , xpro_problem_events.event_type
        , xpro_problem_events.event_json
        , xpro_problem_events.problem_block_id
        , xpro_problem_events.answers
        , xpro_problem_events.attempt
        , xpro_problem_events.success
        , xpro_problem_events.grade
        , xpro_problem_events.max_grade
        , xpro_problem_events.event_timestamp
    from xpro_problem_events

    union all

    select
        '' as user_fk
        , 'residential' as platform
        , mitxresidential_problem_events.user_id as openedx_user_id
        , mitxresidential_problem_events.courserun_readable_id
        , mitxresidential_problem_events.event_type
        , mitxresidential_problem_events.event_json
        , mitxresidential_problem_events.problem_block_id
        , mitxresidential_problem_events.answers
        , mitxresidential_problem_events.attempt
        , mitxresidential_problem_events.success
        , mitxresidential_problem_events.grade
        , mitxresidential_problem_events.max_grade
        , mitxresidential_problem_events.event_timestamp
    from mitxresidential_problem_events

    union all

    select
        '' as user_fk
        , 'edxorg' as platform
        , edxorg_problem_events.user_id as openedx_user_id
        , edxorg_problem_events.courserun_readable_id
        , edxorg_problem_events.event_type
        , edxorg_problem_events.event_json
        , edxorg_problem_events.problem_block_id
        , edxorg_problem_events.answers
        , edxorg_problem_events.attempt
        , edxorg_problem_events.success
        , edxorg_problem_events.grade
        , edxorg_problem_events.max_grade
        , edxorg_problem_events.event_timestamp
    from edxorg_problem_events
)

select

    user_fk
    , openedx_user_id
    , platform
    , courserun_readable_id
    , event_type
    , problem_block_id as problem_block_fk
    , answers
    , attempt
    , success
    , grade
    , max_grade
    , event_timestamp
    , event_json

from combined
