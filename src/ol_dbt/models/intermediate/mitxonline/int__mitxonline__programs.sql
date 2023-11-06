-- Program information for MITx Online

with programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

select
    program_id
    , program_title
    , program_is_live
    , program_readable_id
    , program_type
    , program_is_dedp
    , program_is_micromasters
from programs
