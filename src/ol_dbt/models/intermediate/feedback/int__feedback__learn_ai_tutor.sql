with chatbot as (
    select * from {{ ref('int__learn_ai__chatbot') }}
)

, human_turns as (
    select
        chatbot.djangocheckpoint_id
        , chatbot.chatsession_thread_id
        , chatbot.chatsession_agent
        , chatbot.chatsession_title
        , chatbot.chatsession_object_id
        , chatbot.user_global_id
        , chatbot.human_message
        , chatbot.rating
        , chatbot.checkpoint_source
        , chatbot.checkpoint_type
        , chatbot.checkpoint_step
        , coalesce(chatbot.checkpoint_created_on, chatbot.chatsession_created_on)
            as occurred_at
        , chatbot.chatsession_updated_on
        , chatbot.courserun_readable_id
        , row_number() over (
            partition by chatbot.chatsession_thread_id
            order by chatbot.checkpoint_step, chatbot.djangocheckpoint_id
        ) as turn_index
    from chatbot
    where chatbot.human_message is not null
)

select
    'learn_ai_tutor' as source_slug
    , human_turns.occurred_at
    , cast(human_turns.djangocheckpoint_id as varchar) as source_record_ref
    , human_turns.human_message as text
    , human_turns.chatsession_title as title
    , human_turns.chatsession_thread_id as conversation_ref
    , human_turns.turn_index
    , human_turns.turn_index = 1 as is_conversation_opening
    , human_turns.user_global_id as subject_user_ref
    , cast(null as varchar) as source_url
    , 'chat' as channel_slug
    , human_turns.courserun_readable_id
    , cast(null as varchar) as platform
    , case human_turns.chatsession_agent
        when 'TutorBot' then 'courseware_block'
        when 'VideoGPTBot' then 'courseware_block'
        when 'SyllabusBot' then 'course'
        when 'ResourceRecommendationBot' then 'resource'
    end as subject_type
    , human_turns.chatsession_object_id as subject_ref
    , cast(null as varchar) as subject_url
    , human_turns.rating as explicit_rating
    , human_turns.occurred_at as created_at
    , human_turns.chatsession_updated_on as updated_at
    , json_object(
        'chatsession_agent': human_turns.chatsession_agent
        , 'checkpoint_source': human_turns.checkpoint_source
        , 'checkpoint_type': human_turns.checkpoint_type
    ) as source_metadata
from human_turns
