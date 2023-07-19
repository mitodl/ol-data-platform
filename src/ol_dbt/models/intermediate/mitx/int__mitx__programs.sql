-- MITx Program Information

with micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

select
    micromasters_programs.program_id as micromasters_program_id
    , null as mitxonline_program_id
    , micromasters_programs.program_description
    , micromasters_programs.program_title
    , true as is_micromasters_program
    , false as is_dedp_program
from micromasters_programs
where micromasters_programs.program_id != {{ var("dedp_micromasters_program_id") }}
union all
select
    case
        when mitxonline_programs.program_id = {{ var("dedp_mitxonline_international_development_program_id") }}
            then {{ var("dedp_micromasters_program_id") }}
    end as micromasters_program_id
    , mitxonline_programs.program_id as mitxonline_program_id
    , null as program_description
    , mitxonline_programs.program_title
    , case
        when
            mitxonline_programs.program_id in
            (
                {{ var("dedp_mitxonline_public_policy_program_id") }}
                , {{ var("dedp_mitxonline_international_development_program_id") }}
            ) then true
        else false
    end as is_micromasters_program
    , case
        when
            mitxonline_programs.program_id in
            (
                {{ var("dedp_mitxonline_public_policy_program_id") }}
                , {{ var("dedp_mitxonline_international_development_program_id") }}
            ) then true
        else false
    end as is_dedp_program
from mitxonline_programs
