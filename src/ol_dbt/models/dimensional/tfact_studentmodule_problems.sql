with mitxonline_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxonline__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id'
    ) }}
)

, mitxpro_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxpro__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id'
    ) }}
)

, mitxresidential_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxresidential__openedx__courseware_studentmodule'),
        ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended'),
        'user_id'
    ) }}
)

, users as (
    select * from {{ ref('dim_user') }}
)

, content as (
    select * from {{ ref('dim_course_content') }}
)

, mitxonline_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_submission' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxonline_studentmodule_problems as sm
    left join users on sm.user_id = users.mitxonline_openedx_user_id
    left join content on sm.coursestructure_block_id = content.block_id
)

, mitxpro_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_submission' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxpro_studentmodule_problems as sm
    left join users on sm.user_id = users.mitxpro_openedx_user_id
    left join content on sm.coursestructure_block_id = content.block_id
)

, residential_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'residential' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_submission' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers
        , sm.attempt
        , sm.event_timestamp
        , sm.grade
        , sm.max_grade
        , sm.success
    from mitxresidential_studentmodule_problems as sm
    left join users on sm.user_id = users.residential_openedx_user_id
    left join content on sm.coursestructure_block_id = content.block_id
)

select *
from mitxonline_studentmodule_combined

union all

select *
from mitxpro_studentmodule_combined

union all

select *
from residential_studentmodule_combined
