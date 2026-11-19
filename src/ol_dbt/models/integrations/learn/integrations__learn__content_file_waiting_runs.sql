{#
  The MITx Online and xPRO course runs with files that integrations__learn__content_files
  leaves out until they are ready: the run's newest blocks haven't been through the
  exclusions check yet, or its document or transcript text is from another export.
  Learn leaves a run it reads no rows for as it is, and the ml location's embedding
  sensor reads this so it does the same, rather than rebuilding the run's chunks
  with none.
#}

select distinct
    source_system as etl_source
    , courserun_readable_id as run_readable_id
from {{ ref('int__openedx__content_files') }}
where
    source_system in ('mitxonline', 'xpro')
    and (not is_exclusion_checked or not is_text_current)
