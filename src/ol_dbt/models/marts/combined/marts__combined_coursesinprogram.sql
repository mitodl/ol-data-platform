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

, program_report_key as (
    select * from {{ ref('dim_program_report_key') }}
)

, mitxonline_mapped_titles as (
    select lookup_value as program_title
    from program_report_key
    where lookup_type = 'title' and mapped_by in ('seed', 'title')
)

, program_courses as (
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
    -- A program that dim_program_report_key maps to a MITx Online program takes that
    -- program's courses, as bridge_program_course does.
    left join mitxonline_mapped_titles
        on edxorg_mitx_program_courses.program_title = mitxonline_mapped_titles.program_title
    left join mitx__courses
        on
            replace(edxorg_mitx_program_courses.course_readable_id, 'MITx/', '')
            = replace(mitx__courses.course_readable_id, 'course-v1:MITxT+', '')
    where mitxonline_mapped_titles.program_title is null

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
)

-- The program the row counts toward in program reports: its MITx Online or xPRO readable
-- id when it has one, otherwise its title, as dim_program_report_key maps it
select
    program_courses.platform
    , program_courses.course_title
    , program_courses.program_title
    , program_courses.program_name
    , program_courses.program_id
    , program_courses.course_readable_id
    , program_courses.program_readable_id
    , coalesce(by_readable_id.program_key, by_title.program_key) as program_report_key
    , coalesce(by_readable_id.program_readable_id, by_title.program_readable_id) as report_program_readable_id
    , coalesce(by_readable_id.program_title, by_title.program_title) as report_program_title
from program_courses
left join program_report_key as by_readable_id
    on
        by_readable_id.lookup_type = 'readable_id'
        and program_courses.program_readable_id = by_readable_id.lookup_value
left join program_report_key as by_title
    on
        by_readable_id.lookup_value is null
        and by_title.lookup_type = 'title'
        and program_courses.program_title = by_title.lookup_value
