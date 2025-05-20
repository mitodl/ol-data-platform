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
    select * from {{ ref('dim_user') }}
)

, platform as (
    select * from {{ ref('dim_platform') }}
)

, combined as (
    select
        'mitxonline' as platform
        , users.user_pk as user_fk
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
    left join users
        on
            mitxonline_problem_events.openedx_user_id = users.mitxonline_openedx_user_id
            and mitxonline_problem_events.user_username = users.user_mitxonline_username

    union all

    select
        'mitxpro' as platform
        , users.user_pk as user_fk
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
    left join users
        on
            xpro_problem_events.openedx_user_id = users.mitxpro_openedx_user_id
            and xpro_problem_events.user_username = users.user_mitxpro_username

    union all

    select
        'residential' as platform
        , users.user_pk as user_fk
        , mitxresidential_problem_events.user_id
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
    left join users
        on
            mitxresidential_problem_events.user_id = users.residential_openedx_user_id
            and mitxresidential_problem_events.user_username = users.user_residential_username

    union all

    select
        'edxorg' as platform
        , users.user_pk as user_fk
        , edxorg_problem_events.user_id
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
    left join users
        on
            edxorg_problem_events.user_id = users.edxorg_openedx_user_id
            and edxorg_problem_events.user_username = users.user_edxorg_username
)

select
    platform.platform_pk as platform_fk
    , combined.user_fk
    , combined.platform
    , combined.openedx_user_id
    , combined.courserun_readable_id
    , combined.event_type
    , combined.problem_block_id as problem_block_fk
    , combined.answers
    , combined.attempt
    , combined.success
    , combined.grade
    , combined.max_grade
    , combined.event_timestamp
    , combined.event_json
from combined
left join platform on combined.platform = platform.platform_readable_id
