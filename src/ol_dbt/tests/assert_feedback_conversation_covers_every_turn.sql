-- Every turn's conversation must resolve to a conversation row, so a conversation cannot
-- go missing from the analysis fact and quietly drop its turns out of every cluster.
-- Compound because conversation_id is source-native and can collide across sources.
select
    feedback.feedback_source_fk
    , feedback.conversation_id
from {{ ref('tfact_feedback') }} as feedback
left join {{ ref('afact_feedback_conversation') }} as conversation
    on feedback.conversation_id = conversation.conversation_id
    and feedback.feedback_source_fk = conversation.feedback_source_fk
where conversation.feedback_conversation_pk is null
