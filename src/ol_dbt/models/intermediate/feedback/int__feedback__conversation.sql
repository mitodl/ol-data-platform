-- The ML batch's input: one row per conversation with its kept turns assembled in
-- turn_index order. It exists so the summarizer and embedder never re-derive conversation
-- assembly in Python, and so the assembly logic is testable in dbt.
--
-- Reads the fact rather than int__feedback__unioned because the text must be the REDACTED
-- text (design 7), which the feedback_redacted asset supplies to tfact_feedback. Until
-- that asset lands conversation_text is null while the counts and lengths are real.
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
    feedback_source.source_slug
    , feedback.conversation_id as conversation_ref
    , count(*) as turn_count
    , sum(feedback.feedback_text_chars) as conversation_text_chars
    , {{ array_join(
        "array_agg(feedback.feedback_text order by feedback.turn_index)",
        "\n---\n"
    ) }} as conversation_text
from feedback
inner join feedback_source
    on feedback.feedback_source_fk = feedback_source.feedback_source_pk
group by feedback_source.source_slug, feedback.conversation_id
