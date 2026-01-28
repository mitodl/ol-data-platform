with mitx_programs as (
    select * from {{ ref('int__mitx__program_requirements') }}
)

, edxorg_mitx_program_courses as (
    select * from {{ ref('int__edxorg__mitx_program_courses') }}
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

select
    '{{ var("mitxonline") }}' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , coalesce(mitxonline__programs.program_name, mitx_programs.program_title) as program_name
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
    '{{ var("edxorg") }}' as platform
    , mitx__courses.course_title
    , mitx_programs.program_title
    , mitx_programs.program_title as program_name
    , mitx_programs.micromasters_program_id as program_id
    , mitx__courses.course_readable_id
    , null as program_readable_id
from mitx_programs
inner join mitx__courses
    on mitx_programs.course_number = mitx__courses.course_number
where mitx__courses.is_on_mitxonline = false

union all

select
    '{{ var("edxorg") }}' as platform
    , edxorg_mitx_program_courses.course_title
    , edxorg_mitx_program_courses.program_title
    , edxorg_mitx_program_courses.program_name
    , null as program_id
    , coalesce(mitx__courses.course_readable_id, edxorg_mitx_program_courses.course_readable_id) as course_readable_id
    , null as program_readable_id
from edxorg_mitx_program_courses
left join mitx_programs
    on edxorg_mitx_program_courses.program_name = mitx_programs.program_title
left join mitx__courses
    on
        replace(edxorg_mitx_program_courses.course_readable_id, 'MITx/', '')
        = replace(mitx__courses.course_readable_id, 'course-v1:MITxT+', '')
where mitx_programs.program_title is null

union all

select
    '{{ var("mitxpro") }}' as platform
    , mitxpro__courses.course_title
    , mitxpro__programs.program_title
    , mitxpro__programs.program_title as program_name
    , mitxpro__courses.program_id
    , mitxpro__courses.course_readable_id
    , mitxpro__programs.program_readable_id
from mitxpro__courses
inner join mitxpro__programs
    on mitxpro__courses.program_id = mitxpro__programs.program_id
