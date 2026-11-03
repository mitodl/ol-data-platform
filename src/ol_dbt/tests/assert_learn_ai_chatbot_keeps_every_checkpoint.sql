-- Staged checkpoints with a chat session that int__learn_ai__chatbot lost. Skips ones newer
-- than its last build, since staging can be rebuilt after it, and checks every one when it is empty.
with chatbot as (
    select djangocheckpoint_id from {{ ref('int__learn_ai__chatbot') }}
)

, last_build as (
    select max(djangocheckpoint_id) as max_djangocheckpoint_id from chatbot
)

select checkpoint.djangocheckpoint_id
from {{ ref('stg__learn_ai__app__postgres__chatbots_djangocheckpoint') }} as checkpoint
inner join {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }} as chatsession
    on checkpoint.chatsession_thread_id = chatsession.chatsession_thread_id
cross join last_build
where
    (
        last_build.max_djangocheckpoint_id is null
        or checkpoint.djangocheckpoint_id <= last_build.max_djangocheckpoint_id
    )
    and checkpoint.djangocheckpoint_id not in (select chatbot.djangocheckpoint_id from chatbot)
