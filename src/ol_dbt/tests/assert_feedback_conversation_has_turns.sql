-- The mirror of assert_feedback_conversation_covers_every_turn: every conversation row
-- must resolve to at least one turn. Compound because conversation_id is source-native
-- and can collide across sources; dbt_expectations has no column-pair existence test, so
-- this is the singular-test form of one.
select
    conversation.feedback_source_fk
    , conversation.conversation_id
from {{ ref('afact_feedback_conversation') }} as conversation
left join {{ ref('tfact_feedback') }} as feedback
    on conversation.conversation_id = feedback.conversation_id
    and conversation.feedback_source_fk = feedback.feedback_source_fk
where feedback.feedback_pk is null
