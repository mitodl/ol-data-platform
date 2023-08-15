with micromasters_program_requirements as (
    select * from {{ ref('int__micromasters__program_requirements') }}
)

, mitxonline_program_requirements as (
    select * from {{ ref('int__mitxonline__program_requirements') }}
)

, courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

select
    programs.micromasters_program_id
    , programs.mitxonline_program_id
    , courses.course_number
    , programs.program_title
    , mitxonline_program_requirements.programrequirement_requirement_id as mitxonline_programrequirement_requirement_id
    , null as micromasters_electiveset_id
    , mitxonline_program_requirements.programrequirement_type
    , mitxonline_program_requirements.programrequirement_title
    , mitxonline_program_requirements.electiveset_required_number
    , mitxonline_program_requirements.programrequirement_is_a_nested_requirement
        as programrequirement_is_a_nested_requirement
    , mitxonline_program_requirements.programrequirement_parent_requirement_id
        as mitxonline_programrequirement_parent_requirement_id
    , mitxonline_program_requirements.program_num_required_courses
from programs
inner join mitxonline_program_requirements
    on programs.mitxonline_program_id = mitxonline_program_requirements.program_id
inner join courses
    on mitxonline_program_requirements.course_id = courses.mitxonline_course_id
where programs.mitxonline_program_id is not null

union all

select
    programs.micromasters_program_id
    , programs.mitxonline_program_id
    , courses.course_number
    , programs.program_title
    , null as mitxonline_programrequirement_requirement_id
    , micromasters_program_requirements.electiveset_id as micromasters_electiveset_id
    , micromasters_program_requirements.programrequirement_type
    , micromasters_program_requirements.programrequirement_type as programrequirement_title
    , micromasters_program_requirements.electiveset_required_number
    , false as programrequirement_is_a_nested_requirement
    , null as mitxonline_programrequirement_parent_requirement_id
    , micromasters_program_requirements.program_num_required_courses
from programs
inner join micromasters_program_requirements
    on programs.micromasters_program_id = micromasters_program_requirements.program_id
inner join courses
    on micromasters_program_requirements.course_id = courses.micromasters_course_id
where programs.mitxonline_program_id is null
