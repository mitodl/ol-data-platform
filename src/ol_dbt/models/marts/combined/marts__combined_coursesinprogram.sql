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

select
    'MITx Online' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , mitx__courses.mitxonline_course_id as course_id
    , mitx_programs.mitxonline_program_id as program_id
    , mitx__courses.course_readable_id
from mitx_programs
inner join mitx__courses
    on mitx_programs.course_number = mitx__courses.course_number
where mitx__courses.is_on_mitxonline = true

union all

select
    'edX.org' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , mitx__courses.mitxonline_course_id as course_id
    , mitx_programs.mitxonline_program_id as program_id
    , mitx__courses.course_readable_id
from mitx_programs
inner join mitx__courses
    on mitx_programs.course_number = mitx__courses.course_number
where mitx__courses.is_on_edxorg = true

union all

select
    'xPro' as platform
    , mitxpro__courses.course_title
    , mitxpro__programs.program_title
    , mitxpro__courses.course_id
    , mitxpro__courses.program_id
    , mitxpro__courses.course_readable_id
from mitxpro__courses
left join mitxpro__programs
    on mitxpro__courses.program_id = mitxpro__programs.program_id
