-- The ML batch's input: one row per conversation, kept turns assembled in turn_index
-- order, so assembly stays testable in dbt instead of being re-derived in Python.
--
-- Reads the fact rather than int__feedback__unioned because the text must be REDACTED,
-- which only tfact_feedback carries. conversation_text is null until that asset lands.
with feedback as (
    select * from {{ ref('tfact_feedback') }}
)

, feedback_source as (
    select
        feedback_source_pk
        , source_slug
    from {{ ref('dim_feedback_source') }}
)

select
    {{ dbt_utils.generate_surrogate_key(
        ["feedback_source.source_slug", "feedback.conversation_id"]
    ) }} as feedback_conversation_pk
    , feedback_source.source_slug
    , feedback.conversation_id as conversation_ref
    , count(*) as turn_count
    , sum(feedback.feedback_text_chars) as conversation_text_chars
    -- nullif because array_join drops null elements: with the redaction stub in place
    -- every turn's text is null, and the join yields '' rather than null. An empty
    -- string reads as real content to the summarizer's skip rule.
    , nullif({{ array_join(
        "array_agg(feedback.feedback_text order by feedback.turn_index)",
        "\n---\n"
    ) }}, '') as conversation_text
from feedback
inner join feedback_source
    on feedback.feedback_source_fk = feedback_source.feedback_source_pk
group by feedback_source.source_slug, feedback.conversation_id
