-- Adding forum/ORA is a new CTE plus one `union all`, with no change to the fact.
--
-- This model carries RAW text: Presidio is Python, so masking is its own Dagster asset
-- (feedback_redacted) between here and tfact_feedback.
with zendesk as (
    select * from {{ ref('int__feedback__zendesk') }}
)

, learn_ai_tutor as (
    select * from {{ ref('int__feedback__learn_ai_tutor') }}
)

select
    source_slug
    , occurred_at
    , source_record_ref
    , text
    , title
    , conversation_ref
    , turn_index
    , is_conversation_opening
    , subject_user_ref
    , source_url
    , channel_slug
    , courserun_readable_id
    , block_id
    , platform
    , subject_type
    , subject_ref
    , subject_url
    , explicit_rating
    , created_at
    , updated_at
    , ticket_tags as source_tags
    , source_metadata
    -- measured before masking so the metric survives redaction
    , length(text) as feedback_text_chars
from zendesk

union all

select
    source_slug
    , occurred_at
    , source_record_ref
    , text
    , title
    , conversation_ref
    , turn_index
    , is_conversation_opening
    , subject_user_ref
    , source_url
    , channel_slug
    , courserun_readable_id
    , block_id
    , platform
    , subject_type
    , subject_ref
    , subject_url
    , explicit_rating
    , created_at
    , updated_at
    , {{ null_varchar_array() }} as source_tags
    , source_metadata
    , length(text) as feedback_text_chars
from learn_ai_tutor
