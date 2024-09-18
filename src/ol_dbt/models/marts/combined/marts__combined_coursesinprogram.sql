with mitx_programs as (
    select * from {{ ref('int__mitx__program_requirements') }}
)

, mitx__courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxpro__courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, mitxpro__programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxonline__programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, micromasters__program_requirements as (
    select * from {{ ref('int__micromasters__program_requirements') }}
)

select
    'MITx Online' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , mitx__courses.mitxonline_course_id as course_id
    , mitx_programs.mitxonline_program_id as program_id
    , mitx__courses.course_readable_id
    , mitxonline__programs.program_readable_id
from mitx_programs
inner join mitx__courses
    on mitx_programs.course_number = mitx__courses.course_number
left join mitxonline__programs
    on mitx_programs.mitxonline_program_id = mitxonline__programs.program_id
where mitx__courses.is_on_mitxonline = true

union all

select
    'edX.org' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , micromasters__program_requirements.course_id
    , mitx_programs.micromasters_program_id as program_id
    , mitx__courses.course_readable_id
    , null as program_readable_id
from mitx_programs
inner join mitx__courses
    on mitx_programs.course_number = mitx__courses.course_number
left join micromasters__program_requirements
    on mitx_programs.micromasters_program_id = micromasters__program_requirements.program_id
where mitx__courses.is_on_edxorg = true

union all

select
    'xPro' as platform
    , mitxpro__courses.course_title
    , mitxpro__programs.program_title
    , mitxpro__courses.course_id
    , mitxpro__courses.program_id
    , mitxpro__courses.course_readable_id
    , mitxpro__programs.program_readable_id
from mitxpro__courses
left join mitxpro__programs
    on mitxpro__courses.program_id = mitxpro__programs.program_id
