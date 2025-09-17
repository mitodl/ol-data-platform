with chatbot as (
    select
        *
        , row_number() over (
            partition by chatsession_thread_id
            order by checkpoint_step
        ) as message_index
    from {{ ref('int__learn_ai__chatbot') }}
    where coalesce(agent_message, '') != '' or human_message is not null
)

, tutorbot as (
    -- Get the most recent tutorbotoutput entry for each chatsession_thread_id
    -- since there can be multiple entries per thread and we want only the last one for the
    -- complete chat history
    select * from (
        select
            *
            , json_parse(json_extract_scalar(tutorbot_chat_json, '$')) as chat_json
            , row_number() over (
                partition by chatsession_thread_id
                order by tutorbotoutput_id desc
            ) as row_num
        from {{ ref("int__learn_ai__tutorbot") }}
    )
    where row_num = 1
)

, tutorbot_flatten as (
    select
        tutorbot.*
        , t.idx as message_index
        , case
            when json_extract_scalar(t.element, '$.type') = 'HumanMessage'
                then json_extract_scalar(t.element, '$.content')
        end as human_message
        , case
            when json_extract_scalar(t.element, '$.type') = 'AIMessage'
                then json_extract_scalar(t.element, '$.content')
        end as agent_message
    from tutorbot
    cross join
        unnest(cast(json_extract(tutorbot.chat_json, '$.chat_history') as array<json>))
    with ordinality as t(element, idx) -- noqa: PRS
)

select
    chatsession_agent as ai_agent
    , chatsession_object_id as resource_id
    , chatsession_thread_id as thread_id
    , human_message
    , agent_message as ai_message
    , chatsession_created_on as created_on
    , message_index
from chatbot

union all

select
    tutorbot_flatten.chatsession_agent as ai_agent
    , tutorbot_flatten.edx_module_id as resource_id
    , tutorbot_flatten.chatsession_thread_id as thread_id
    , tutorbot_flatten.human_message
    , tutorbot_flatten.agent_message as ai_message
    , tutorbot_flatten.chatsession_created_on as created_on
    , tutorbot_flatten.message_index
from tutorbot_flatten
left join chatbot
    on tutorbot_flatten.chatsession_thread_id = chatbot.chatsession_thread_id
where chatbot.chatsession_thread_id is null
