{#
  integrations__learn__mitpe_runs
  Published runs of MIT Professional Education courses and programs, for MIT Learn
  webhook delivery. Unpublished runs are left out, which MIT Learn treats as
  unpublished. So are runs that one of the feed's run lists is too short to reach,
  because MIT Learn's ETL zips the lists and never sees them. Dates are ISO 8601 UTC
  instants of midnight US Eastern.
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
where is_published and is_in_all_run_lists
