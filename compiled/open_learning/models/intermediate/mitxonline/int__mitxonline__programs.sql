-- Program information for MITx Online

with programs as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__courses_program
)

select
    program_id
    , program_title
    , program_is_live
    , program_readable_id
from programs
