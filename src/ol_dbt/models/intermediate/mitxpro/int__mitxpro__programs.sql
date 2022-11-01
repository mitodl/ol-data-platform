-- Program information for MITxPro

with courses as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_program') }}
)

select
    program_id
    , program_title
    , program_is_live
    , program_readable_id
from courses
