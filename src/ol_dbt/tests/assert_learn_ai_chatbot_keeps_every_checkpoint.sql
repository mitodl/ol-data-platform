-- Staged checkpoints with a chat session that int__learn_ai__chatbot lost. Skips ones newer
-- than its last build, since staging can be rebuilt after it.
with chatbot as (
    select djangocheckpoint_id from {{ ref('int__learn_ai__chatbot') }}
)

select checkpoint.djangocheckpoint_id
from {{ ref('stg__learn_ai__app__postgres__chatbots_djangocheckpoint') }} as checkpoint
inner join {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }} as chatsession
    on checkpoint.chatsession_thread_id = chatsession.chatsession_thread_id
where
    checkpoint.djangocheckpoint_id <= (select max(chatbot.djangocheckpoint_id) from chatbot)
    and checkpoint.djangocheckpoint_id not in (select chatbot.djangocheckpoint_id from chatbot)
