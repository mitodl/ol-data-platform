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
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_id
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
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_id
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
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_id
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
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name
        , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_id
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

, mitxonline_users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, xpro_users as (
    select * from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)


, combined as (
    select
        'mitxonline' as platform
        , coalesce(mitxonline_users.user_id, mitxonline_problem_events.openedx_user_id) as user_id
        , mitxonline_problem_events.courserun_readable_id
        , mitxonline_problem_events.event_type
        , mitxonline_problem_events.problem_id
        , mitxonline_problem_events.answers
        , mitxonline_problem_events.attempt
        , mitxonline_problem_events.success
        , mitxonline_problem_events.grade
        , mitxonline_problem_events.max_grade
        , mitxonline_problem_events.event_timestamp
    from mitxonline_problem_events
    left join mitxonline_users on mitxonline_problem_events.user_username = mitxonline_users.user_username

    union all

    select
        'mitxpro' as platform
        , coalesce(xpro_users.user_id, xpro_problem_events.openedx_user_id) as user_id
        , xpro_problem_events.courserun_readable_id
        , xpro_problem_events.event_type
        , xpro_problem_events.problem_id
        , xpro_problem_events.answers
        , xpro_problem_events.attempt
        , xpro_problem_events.success
        , xpro_problem_events.grade
        , xpro_problem_events.max_grade
        , xpro_problem_events.event_timestamp
    from xpro_problem_events
    left join xpro_users on xpro_problem_events.user_username = xpro_users.user_username

    union all

    select
        'residential' as platform
        , user_id
        , courserun_readable_id
        , event_type
        , problem_id
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
        , problem_id
        , answers
        , attempt
        , success
        , grade
        , max_grade
        , event_timestamp
    from edxorg_problem_events
)

select
     {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , platform as platform_id
    , courserun_readable_id
    , event_type
    , problem_id
    , answers
    , attempt
    , success
    , grade
    , max_grade
    , event_timestamp

from combined
