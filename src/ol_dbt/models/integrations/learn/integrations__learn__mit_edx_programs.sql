{#
  integrations__learn__mit_edx_programs
  The edX.org programs MIT Learn lists, for webhook delivery: MITx and MITx_PRO
  programs that edX currently lists as active, excluding MicroMasters, which MIT Learn
  deleted and has no destination for. One row per program with the single run MIT
  Learn models a program as. Instructors are in
  integrations__learn__mit_edx_program_instructors.
  Contract: docs/learn_marts_contract.md
#}

with programs as (
    select * from {{ ref('int__edxorg__mitx_learn_programs') }}
)

, rollups as (
    select * from {{ ref('int__edxorg__mitx_learn_program_rollups') }}
)

select
    programs.readable_id
    , programs.title
    , programs.description
    , programs.url
    , programs.image_url
    , programs.last_modified
    , programs.level
    , programs.start_date
    , programs.end_date
    , programs.enrollment_start
    , programs.enrollment_end
    , programs.price
    , programs.currency
    , programs.pace
    , programs.availability
    , rollups.topics
    , rollups.duration
    , rollups.min_weeks
    , rollups.max_weeks
    , rollups.time_commitment
    , rollups.min_weekly_hours
    , rollups.max_weekly_hours
    , programs.course_readable_ids
    , programs.retrieved_at
    , 'mit_edx' as etl_source
    , 'edx' as platform
    , 'program' as resource_type
from programs
inner join rollups on programs.readable_id = rollups.readable_id
