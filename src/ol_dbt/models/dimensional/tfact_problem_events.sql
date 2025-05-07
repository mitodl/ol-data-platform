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

, mitxonline_studentmodule as (
	select
		studentmodule_id,
		, courserun_readable_id
		, coursestructure_block_id
		, coursestructure_block_category
		, user_id
		, studentmodule_state_data
		, studentmodule_problem_grade
		, studentmodule_problem_max_grade
		, studentmodule_created_on
		, studentmodule_updated_on
	from {{ ref('stg__mitxonline__openedx__courseware_studentmodule') }}
)

, mitxonline_studentmodulehistoryextended as (
	select
		studentmodulehistoryextended_id
		, studentmodule_id
		, studentmodule_state_data
		, studentmodule_problem_grade
		, studentmodule_problem_max_grade
		, studentmodule_created_on
	from {{ ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended') }}
)

, mitxonline_blockcompletion as (
    select
        blockcompletion_id
		, user_id
		, blockcompletion_created_on
		, blockcompletion_updated_on
		, block_fk
		, block_category
		, block_completed
		, courserun_readable_id
    from {{ ref('stg__mitxonline__openedx__blockcompletion') }}
)

, mitxonline_studentmodule_combined as (
    select
        'mitxonline'
		, sm.user_id
		, sm.courserun_readable_id
		, bc.block_category as event_type
		, bc.block_fk as problem_block_fk
		, as answers
		, as attempt
		, as success
		, as grade
		, as max_grade
		, as event_timestamp
		, as event_json
	from mitxonline_studentmodule sm
	inner join mitxonline_studentmodulehistoryextended smhe on sm.studentmodule_id = smhe.studentmodule_id
	inner join mitxonline_blockcompletion bc  on sm.user_id = bc.user_id
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
    , problem_block_id as problem_block_fk
    , answers
    , attempt
    , success
    , grade
    , max_grade
    , event_timestamp
    , event_json

from combined
