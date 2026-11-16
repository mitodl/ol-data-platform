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

, edxorg_requirement_courses as (
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
)

-- edX.org's programs API can list one program under two titles (Finance and MIT Finance)
-- that map to one report key, so a course keeps one title per key
, edxorg_program_api_course_titles as (
    select
        edxorg_mitx_program_courses.*
        , row_number() over (
            partition by
                edxorg_mitx_program_courses.mitx_course_readable_id
                , coalesce(program_report_key.program_key, edxorg_mitx_program_courses.program_title)
            order by
                case
                    when edxorg_mitx_program_courses.program_title = edxorg_mitx_program_courses.program_name
                        then 0
                    else 1
                end nulls last
                , edxorg_mitx_program_courses.program_title nulls last
        ) as title_rank
    from edxorg_mitx_program_courses
    left join program_report_key
        on
            program_report_key.lookup_type = 'title'
            and edxorg_mitx_program_courses.program_title = program_report_key.lookup_value
)

, edxorg_program_api_courses as (
    select
        '{{ var("edxorg") }}' as platform
        , course_title
        , program_title
        , program_name
        , null as program_id
        , mitx_course_readable_id as course_readable_id
        , null as program_readable_id
    from edxorg_program_api_course_titles
    where title_rank = 1
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

    select * from edxorg_requirement_courses

    union all

    -- Every edX.org MITx program keeps its own courses, also one that moved to MITx
    -- Online, as bridge_program_course does: its edX.org learners took courses the MITx
    -- Online program may not list. A course the program's requirements already list is
    -- left out.
    select edxorg_program_api_courses.*
    from edxorg_program_api_courses
    left join edxorg_requirement_courses
        on
            edxorg_program_api_courses.program_title = edxorg_requirement_courses.program_title
            and edxorg_program_api_courses.course_readable_id = edxorg_requirement_courses.course_readable_id
    where edxorg_requirement_courses.course_readable_id is null

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
