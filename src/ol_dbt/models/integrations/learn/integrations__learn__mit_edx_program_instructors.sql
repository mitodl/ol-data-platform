{#
  integrations__learn__mit_edx_program_instructors
  Instructors of the programs in integrations__learn__mit_edx_programs, for webhook
  delivery, one row per program and instructor in the order MIT Learn lists them.
  Contract: docs/learn_marts_contract.md
#}

select
    program_uuid as readable_id
    , first_name
    , last_name
    , full_name
    , instructor_position
from {{ ref('int__edxorg__mitx_learn_program_instructors') }}
