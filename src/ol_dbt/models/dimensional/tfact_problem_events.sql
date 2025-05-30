{% set problem_events =
    (
    'problem_check'
    , 'showanswer'
    )
%}
-- data from tracking logs
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

, content as (
    select * from {{ ref('dim_course_content') }}
)

-- data from studentmodule and studentmodulehistoryextended
, mitxonline_studentmodule as (
    select
        studentmodule_id
        , courserun_readable_id
        , coursestructure_block_id
        , coursestructure_block_category
        , openedx_user_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
        , studentmodule_updated_on
    from {{ ref('stg__mitxonline__openedx__mysql__courseware_studentmodule') }}
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

, mitxonline_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , users.user_mitxonline_username as user_username
        , sm.openedx_user_id
        , sm.courserun_readable_id
        , 'problem_check' as event_type
        , smhe.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , null as answers -- break out in an intermediate table
        , null as attempt -- break out in an intermediate table
        , null as success -- break out in an intermediate table
        , cast(smhe.studentmodule_problem_grade as varchar) as grade
        , cast(smhe.studentmodule_problem_max_grade as varchar) as max_grade
        , from_iso8601_timestamp_nanos(sm.studentmodule_updated_on) as event_timestamp
    from mitxonline_studentmodule as sm
    inner join mitxonline_studentmodulehistoryextended as smhe on sm.studentmodule_id = smhe.studentmodule_id
    inner join users on sm.openedx_user_id = users.mitxonline_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, mitxpro_studentmodule as (
    select
        studentmodule_id
        , courserun_readable_id
        , coursestructure_block_id
        , coursestructure_block_category
        , openedx_user_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
        , studentmodule_updated_on
    from {{ ref('stg__mitxpro__openedx__mysql__courseware_studentmodule') }}
)

, mitxpro_studentmodulehistoryextended as (
    select
        studentmodulehistoryextended_id
        , studentmodule_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
    from {{ ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended') }}
)

, mitxpro_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , users.user_mitxpro_username as user_username
        , sm.openedx_user_id
        , sm.courserun_readable_id
        , 'problem_check' as event_type
        , smhe.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , null as answers -- break out in an intermediate table
        , null as attempt -- break out in an intermediate table
        , null as success -- break out in an intermediate table
        , cast(smhe.studentmodule_problem_grade as varchar) as grade
        , cast(smhe.studentmodule_problem_max_grade as varchar) as max_grade
        , from_iso8601_timestamp_nanos(sm.studentmodule_updated_on) as event_timestamp
    from mitxpro_studentmodule as sm
    inner join mitxpro_studentmodulehistoryextended as smhe on sm.studentmodule_id = smhe.studentmodule_id
    inner join users on sm.openedx_user_id = users.mitxpro_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, residential_studentmodule as (
    select
        studentmodule_id
        , courserun_readable_id
        , coursestructure_block_id
        , coursestructure_block_category
        , user_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
        , studentmodule_updated_on
    from {{ ref('stg__mitxresidential__openedx__courseware_studentmodule') }}
)

, residential_studentmodulehistoryextended as (
    select
        studentmodulehistoryextended_id
        , studentmodule_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
    from {{ ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended') }}
)

, residential_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , users.user_residential_username as user_username
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , 'problem_check' as event_type
        , smhe.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , null as answers -- break out in an intermediate table
        , null as attempt -- break out in an intermediate table
        , null as success -- break out in an intermediate table
        , cast(smhe.studentmodule_problem_grade as varchar) as grade
        , cast(smhe.studentmodule_problem_max_grade as varchar) as max_grade
        , from_iso8601_timestamp_nanos(sm.studentmodule_updated_on) as event_timestamp
    from residential_studentmodule as sm
    inner join residential_studentmodulehistoryextended as smhe on sm.studentmodule_id = smhe.studentmodule_id
    inner join users on sm.user_id = users.residential_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, edxorg_studentmodule as (
    select
        studentmodule_id
        , courserun_readable_id
        , coursestructure_block_id
        , coursestructure_block_category
        , user_id
        , studentmodule_state_data
        , studentmodule_problem_grade
        , studentmodule_problem_max_grade
        , studentmodule_created_on
        , studentmodule_updated_on
    from {{ ref('stg__edxorg__s3__courseware_studentmodule') }}
)

, edxorg_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , users.user_residential_username as user_username
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , null as answers -- break out in an intermediate table
        , null as attempt -- break out in an intermediate table
        , null as success -- break out in an intermediate table
        , cast(sm.studentmodule_problem_grade as varchar) as grade
        , cast(sm.studentmodule_problem_max_grade as varchar) as max_grade
        , from_iso8601_timestamp_nanos(sm.studentmodule_updated_on) as event_timestamp
    from edxorg_studentmodule as sm
    inner join users on sm.user_id = users.residential_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
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

    union all

    select
        'mitxonline' as platform
        , user_fk
        , openedx_user_id
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
    from mitxonline_studentmodule_combined

    union all

    select
        'mitxpro' as platform
        , user_fk
        , openedx_user_id
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
    from mitxpro_studentmodule_combined

    union all

    select
        'residential' as platform
        , user_fk
        , openedx_user_id
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
    from residential_studentmodule_combined

    union all

    select
        'edxorg' as platform
        , user_fk
        , openedx_user_id
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
    from edxorg_studentmodule_combined
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
