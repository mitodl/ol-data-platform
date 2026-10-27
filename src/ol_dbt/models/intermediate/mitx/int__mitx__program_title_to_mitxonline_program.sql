with mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, edxorg_to_mitxonline_program_mapping as (
    select * from {{ ref('edxorg_to_mitxonline_program_mapping') }}
)

, mapped_by_seed as (
    select
        edxorg_to_mitxonline_program_mapping.edxorg_program_title as program_title
        , mitxonline_programs.program_id as mitxonline_program_id
        , mitxonline_programs.program_readable_id as mitxonline_program_readable_id
        , 'seed' as mapped_by
    from edxorg_to_mitxonline_program_mapping
    inner join mitxonline_programs
        on edxorg_to_mitxonline_program_mapping.mitxonline_program_readable_id = mitxonline_programs.program_readable_id
)

-- A title two MITx Online programs share names neither of them, so it maps to nothing.
, mapped_by_title as (
    select
        program_title
        , min(program_id) as mitxonline_program_id
        , min(program_readable_id) as mitxonline_program_readable_id
        , 'title' as mapped_by
    from mitxonline_programs
    group by program_title
    having count(*) = 1
)

select
    program_title
    , mitxonline_program_id
    , mitxonline_program_readable_id
    , mapped_by
from mapped_by_seed

union all

select
    mapped_by_title.program_title
    , mapped_by_title.mitxonline_program_id
    , mapped_by_title.mitxonline_program_readable_id
    , mapped_by_title.mapped_by
from mapped_by_title
left join mapped_by_seed
    on mapped_by_title.program_title = mapped_by_seed.program_title
where mapped_by_seed.program_title is null
