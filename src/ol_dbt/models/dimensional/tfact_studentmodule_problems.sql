{{ config(
    materialized='incremental',
    unique_key=['platform', 'openedx_user_id', 'courserun_readable_id', 'problem_block_id', 'attempt'],
    incremental_strategy='delete+insert'
) }}

with mitxonline_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxonline__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxonline'
    ) }}
)

, mitxpro_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxpro__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxpro'
    ) }}
)

, mitxresidential_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxresidential__openedx__courseware_studentmodule'),
        ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended'),
        'user_id',
        'residential'
    ) }}
)

, users as (
    select * from {{ ref('dim_user') }}
)

, mitxonline_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , sm.user_id as openedx_user_id
        , users.user_mitxonline_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxonline_studentmodule_problems as sm
    left join users on sm.user_id = users.mitxonline_openedx_user_id
)

, mitxpro_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , sm.user_id as openedx_user_id
        , users.user_mitxpro_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxpro_studentmodule_problems as sm
    left join users on sm.user_id = users.mitxpro_openedx_user_id
)

, residential_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'residential' as platform
        , sm.user_id as openedx_user_id
        , users.user_residential_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxresidential_studentmodule_problems as sm
    left join users on sm.user_id = users.residential_openedx_user_id
)

, combined as (
    select *
    from mitxonline_studentmodule_combined

    union all

    select *
    from mitxpro_studentmodule_combined

    union all

    select *
    from residential_studentmodule_combined
)

-- dedupe the tracking log and student module data based on user_id, course_run, problem, and time
-- The dbt model definition has a test against the same composite unique key.
, deduped_combined as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by platform, openedx_user_id, courserun_readable_id, problem_block_id, attempt
                order by event_timestamp
            ) as rn
        from combined
    )
    where rn = 1
)

select
    user_fk
    , platform
    , openedx_user_id
    , user_username
    , courserun_readable_id
    , studentmodule_id
    , event_type
    , event_json
    , problem_block_id
    , answers
    , attempt
    , event_timestamp
    , grade
    , max_grade
    , success
from deduped_combined
