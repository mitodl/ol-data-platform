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

, combined as (
    select
        'mitxonline' as platform
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

    union all

    select
        'mitxpro' as platform
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
        'residential' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , event_json
        , problem_block_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
    from mitxresidential_problem_events

    union all

    select
        'edxorg' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , event_json
        , problem_block_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
    from edxorg_problem_events
)

select

    platform
    , openedx_user_id
    , courserun_readable_id
    , event_type
    , problem_block_id
    , answers
    , attempt
    , success
    , grade
    , max_grade
    , event_timestamp
    , event_json

from combined
