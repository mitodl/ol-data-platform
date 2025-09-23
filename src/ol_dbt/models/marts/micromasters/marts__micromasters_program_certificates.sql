with program_certificates as (select * from {{ ref("int__micromasters__program_certificates") }})

select *
from program_certificates
