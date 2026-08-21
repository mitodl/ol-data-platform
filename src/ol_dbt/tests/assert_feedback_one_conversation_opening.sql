-- Exactly one opening turn per conversation, which is what makes the first-comment-only
-- view faithfully recoverable as `where is_conversation_opening`. Counted conditionally
-- over every turn, not filtered to openers, so a conversation with no opening turn is
-- caught as well as one with several.
select
    feedback_source_fk
    , conversation_id
    , sum(case when is_conversation_opening then 1 else 0 end) as opening_count
from {{ ref('tfact_feedback') }}
group by feedback_source_fk, conversation_id
having sum(case when is_conversation_opening then 1 else 0 end) != 1
