-- How a source row names a program, and the program it counts toward in program reports.
-- A row with a real readable id (MITx Online or xPRO) looks it up by readable_id. An
-- edX.org or MicroMasters row has none, so it looks up its title.
with mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, mitxpro_programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, edxorg_to_mitxonline_program_mapping as (
    select * from {{ ref('edxorg_to_mitxonline_program_mapping') }}
)

-- Every title an edX.org or MicroMasters row in marts__combined_program_enrollment_detail
-- or marts__combined_coursesinprogram can carry
, source_titles as (
    select program_title, program_type
    from {{ ref('int__edxorg__mitx_program_enrollments') }}

    union all

    select program_title, program_type
    from {{ ref('int__edxorg__mitx_program_courses') }}

    union all

    select program_title, 'MicroMasters' as program_type
    from {{ ref('int__micromasters__program_enrollments') }}

    union all

    select program_title, cast(null as varchar) as program_type
    from {{ ref('int__mitx__program_requirements') }}
)

, distinct_source_titles as (
    select
        program_title
        , min(program_type) as program_type
    from source_titles
    where program_title is not null
    group by program_title
)

-- A title two MITx Online programs share names neither of them.
, unique_mitxonline_titles as (
    select
        program_title
        , min(program_id) as program_id
    from mitxonline_programs
    group by program_title
    having count(*) = 1
)

, title_matches as (
    select
        distinct_source_titles.program_title
        , distinct_source_titles.program_type
        , coalesce(seed_programs.program_id, unique_mitxonline_titles.program_id) as mitxonline_program_id
        , case
            when seed_programs.program_id is not null then 'seed'
            when unique_mitxonline_titles.program_id is not null then 'title'
            else 'none'
        end as mapped_by
    from distinct_source_titles
    left join edxorg_to_mitxonline_program_mapping
        on distinct_source_titles.program_title = edxorg_to_mitxonline_program_mapping.edxorg_program_title
    left join mitxonline_programs as seed_programs
        on edxorg_to_mitxonline_program_mapping.mitxonline_program_readable_id = seed_programs.program_readable_id
    left join unique_mitxonline_titles
        on distinct_source_titles.program_title = unique_mitxonline_titles.program_title
)

select
    'readable_id' as lookup_type
    , program_readable_id as lookup_value
    , program_readable_id as program_key
    , program_readable_id
    , program_title
    , program_name
    , program_type
    , program_id as mitxonline_program_id
    , 'own' as mapped_by
from mitxonline_programs

union all

select
    'readable_id' as lookup_type
    , program_readable_id as lookup_value
    , program_readable_id as program_key
    , program_readable_id
    , program_title
    , program_title as program_name
    , 'Professional Certificate' as program_type
    , cast(null as integer) as mitxonline_program_id
    , 'own' as mapped_by
from mitxpro_programs

union all

-- A title that maps to no MITx Online program keys its own report row, with no readable id.
select
    'title' as lookup_type
    , title_matches.program_title as lookup_value
    , coalesce(mitxonline_programs.program_readable_id, title_matches.program_title) as program_key
    , mitxonline_programs.program_readable_id
    , coalesce(mitxonline_programs.program_title, title_matches.program_title) as program_title
    , coalesce(mitxonline_programs.program_name, title_matches.program_title) as program_name
    , coalesce(mitxonline_programs.program_type, title_matches.program_type) as program_type
    , title_matches.mitxonline_program_id
    , title_matches.mapped_by
from title_matches
left join mitxonline_programs
    on title_matches.mitxonline_program_id = mitxonline_programs.program_id
