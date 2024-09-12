-- Program information for MITx Online

with programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, program_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_programpage') }}
)


select
    programs.program_id
    , programs.program_title
    , programs.program_is_live
    , programs.program_readable_id
    , programs.program_type
    , programs.program_is_dedp
    , programs.program_is_micromasters
    , program_pages.program_description
    , program_pages.program_length
    , program_pages.program_effort
    , program_pages.program_prerequisites
    , program_pages.program_about
    , program_pages.program_what_you_learn
from programs
left join program_pages on programs.program_id = program_pages.program_id
