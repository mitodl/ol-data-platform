-- MITx Program Information

with micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

select
    micromasters_programs.program_id as micromasters_program_id
    , mitxonline_programs.program_id as mitxonline_program_id
    , micromasters_programs.program_description
    , coalesce(mitxonline_programs.program_title, micromasters_programs.program_title) as program_title
from micromasters_programs
full join mitxonline_programs
    on
        replace(replace(micromasters_programs.program_title, ',', ''), ' ', '')
        = replace(replace(mitxonline_programs.program_title, ',', ''), ' ', '')
