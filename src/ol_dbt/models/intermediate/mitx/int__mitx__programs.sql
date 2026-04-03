-- MITx Program Information

with micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, micromasters_programs_view as (

    select
        micromasters_programs.program_id as micromasters_program_id
        , mitxonline_programs.program_id as mitxonline_program_id
        , micromasters_programs.program_description
        , coalesce(mitxonline_programs.program_title, micromasters_programs.program_title) as program_title
        , true as is_micromasters_program
        , false as is_dedp_program
        , mitxonline_programs.program_readable_id
    from micromasters_programs
    full outer join mitxonline_programs
        on mitxonline_programs.program_title = micromasters_programs.program_title
         and mitxonline_programs.program_is_micromasters = true
    where micromasters_programs.program_id != {{ var("dedp_micromasters_program_id") }}
)

select
    micromasters_program_id
    , mitxonline_program_id
    , program_description
    , program_title
    , is_micromasters_program
    , is_dedp_program
    , program_readable_id
from micromasters_programs_view

union all

select
    case
        when mitxonline_programs.program_id = {{ var("dedp_mitxonline_international_development_program_id") }}
            then {{ var("dedp_micromasters_program_id") }}
    end as micromasters_program_id
    , mitxonline_programs.program_id as mitxonline_program_id
    , null as program_description
    , mitxonline_programs.program_title
    , mitxonline_programs.program_is_micromasters as is_micromasters_program
    , mitxonline_programs.program_is_dedp as is_dedp_program
    , mitxonline_programs.program_readable_id
from mitxonline_programs
left join micromasters_programs_view
    on micromasters_programs_view.mitxonline_program_id = mitxonline_programs.program_id
where micromasters_programs_view.micromasters_program_id is null
