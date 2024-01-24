with course_certificates as (
    select * from {{ ref('int__micromasters__course_certificates') }}
)

select *
from course_certificates
