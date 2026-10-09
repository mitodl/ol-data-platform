-- Every staged checkpoint with a chat session must reach int__learn_ai__chatbot; its
-- left joins cannot drop rows, so a missing one means the build lost it.
select checkpoint.djangocheckpoint_id
from {{ ref('stg__learn_ai__app__postgres__chatbots_djangocheckpoint') }} as checkpoint
inner join {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }} as chatsession
    on checkpoint.chatsession_thread_id = chatsession.chatsession_thread_id
left join {{ ref('int__learn_ai__chatbot') }} as chatbot
    on checkpoint.djangocheckpoint_id = chatbot.djangocheckpoint_id
where chatbot.djangocheckpoint_id is null
