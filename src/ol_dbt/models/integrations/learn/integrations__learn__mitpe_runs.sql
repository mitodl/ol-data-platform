{#
  integrations__learn__mitpe_runs
  Published runs of MIT Professional Education courses and programs, for MIT Learn
  webhook delivery. Unpublished runs are left out, which MIT Learn treats as
  unpublished. Dates are ISO 8601 UTC instants of midnight US Eastern.
  Contract: docs/learn_marts_contract.md
#}

select
    readable_id
    , run_id
    , run_position
    , {{ format_timestamp_as_iso8601('start_on') }} as start_date
    , {{ format_timestamp_as_iso8601('end_on') }} as end_date
    , {{ format_timestamp_as_iso8601('enrollment_end_on') }} as enrollment_end
from {{ ref('int__mitpe__learning_resource_runs') }}
where is_published
