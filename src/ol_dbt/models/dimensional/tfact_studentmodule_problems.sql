{{ config(materialized='view') }}
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

, edxorg_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__edxorg__s3__courseware_studentmodule'),
        'null',
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
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers_json as answers
        , sm.attempt
        , sm.studentmodule_updated_on as event_timestamp
        , sm.problem_id
        , cast(sm.studentmodule_problem_grade as varchar) as grade
        , cast(sm.studentmodule_problem_max_grade as varchar) as max_grade
        , case
            when sm.correctness = 'correct' then 'correct'
            when sm.correctness in ('incorrect', 'partially-incorrect') then 'incorrect'
        end as success
    from mitxonline_studentmodule_problems as sm
    inner join users on sm.user_id = users.mitxonline_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, mitxpro_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers_json as answers
        , sm.attempt
        , sm.studentmodule_updated_on as event_timestamp
        , sm.problem_id
        , cast(sm.studentmodule_problem_grade as varchar) as grade
        , cast(sm.studentmodule_problem_max_grade as varchar) as max_grade
        , case
            when sm.correctness = 'correct' then 'correct'
            when sm.correctness in ('incorrect', 'partially-incorrect') then 'incorrect'
        end as success
    from mitxpro_studentmodule_problems as sm
    inner join users on sm.user_id = users.mitxpro_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, residential_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'residential' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers_json as answers
        , sm.attempt
        , sm.studentmodule_updated_on as event_timestamp
        , sm.problem_id
        , cast(sm.studentmodule_problem_grade as varchar) as grade
        , cast(sm.studentmodule_problem_max_grade as varchar) as max_grade
        , case
            when sm.correctness = 'correct' then 'correct'
            when sm.correctness in ('incorrect', 'partially-incorrect') then 'incorrect'
        end as success
    from mitxresidential_studentmodule_problems as sm
    inner join users on sm.user_id = users.residential_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

, edxorg_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'edxorg' as platform
        , sm.user_id as openedx_user_id
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , 'problem_check' as event_type
        , sm.studentmodule_state_data as event_json
        , content.block_title as problem_name
        , sm.coursestructure_block_id as problem_block_id
        , sm.answers_json as answers
        , sm.attempt
        , sm.studentmodule_updated_on as event_timestamp
        , sm.problem_id
        , cast(sm.studentmodule_problem_grade as varchar) as grade
        , cast(sm.studentmodule_problem_max_grade as varchar) as max_grade
        , case
            when sm.correctness = 'correct' then 'correct'
            when sm.correctness in ('incorrect', 'partially-incorrect') then 'incorrect'
        end as success
    from edxorg_studentmodule_problems as sm
    inner join users on sm.user_id = users.edxorg_openedx_user_id
    inner join content on sm.coursestructure_block_id = content.block_id
)

select *
from mitxonline_studentmodule_combined

union all

select *
from mitxpro_studentmodule_combined

union all

select *
from residential_studentmodule_combined

union all

select *
from edxorg_studentmodule_combined
