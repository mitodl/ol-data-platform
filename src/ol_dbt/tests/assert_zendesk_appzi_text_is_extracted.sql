{{ config(severity="warn") }}

-- A row here is an Appzi email the patterns in macros/appzi.sql no longer match.
select
    source_record_ref
    , conversation_ref
    , occurred_at
from {{ ref('int__feedback__zendesk') }}
where {{ is_appzi_email('text') }}
