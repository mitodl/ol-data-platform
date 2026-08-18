-- Exactly one opening turn per conversation. This is the guard that the previous
-- first-comment-only view is faithfully recoverable as `where is_conversation_opening`.
select
    feedback_source_fk
    , conversation_id
    , count(*) as opening_count
from {{ ref('tfact_feedback') }}
where is_conversation_opening
group by feedback_source_fk, conversation_id
having count(*) != 1
