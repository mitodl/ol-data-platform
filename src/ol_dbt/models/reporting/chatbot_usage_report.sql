with
    chatbot as (
        select *, row_number() over (partition by chatsession_thread_id order by djangocheckpoint_id) as message_index
        from
            (
                select *
                from {{ ref("int__learn_ai__chatbot") }}
                where coalesce(agent_message, '') != '' or human_message is not null
            )
    ),
    tutorbot as (
        select *, json_parse(json_extract_scalar(tutorbot_chat_json, '$')) as chat_json
        from {{ ref("int__learn_ai__tutorbot") }}
    ),
    tutorbot_flatten as (
        select
            tutorbot.*,
            t.idx as message_index,
            case
                when json_extract_scalar(t.element, '$.type') = 'HumanMessage'
                then json_extract_scalar(t.element, '$.content')
            end as human_message,
            case
                when json_extract_scalar(t.element, '$.type') = 'AIMessage'
                then json_extract_scalar(t.element, '$.content')
            end as agent_message
        from tutorbot
        cross join unnest(cast(json_extract(tutorbot.chat_json, '$.chat_history') as array<json>))
        with ordinality as t(element, idx)  -- noqa: PRS
    ),
    tutorbot_deduplicated as (
        select
            tutorbotoutput_id,
            chatsession_agent,
            edx_module_id,
            chatsession_thread_id,
            human_message,
            agent_message,
            chatsession_created_on,
            row_number() over (
                partition by chatsession_thread_id order by tutorbotoutput_id, message_index
            ) as message_index
        from
            (
                select
                    *,
                    row_number() over (
                        partition by chatsession_thread_id, human_message, agent_message
                        order by tutorbotoutput_id, message_index
                    ) as message_occurrence
                from tutorbot_flatten
            )
        where message_occurrence = 1
    )

select
    chatsession_agent as ai_agent,
    chatsession_object_id as resource_id,
    chatsession_thread_id as thread_id,
    human_message,
    agent_message as ai_message,
    chatsession_created_on as created_on,
    message_index
from chatbot

union all

select
    tutorbot_deduplicated.chatsession_agent as ai_agent,
    tutorbot_deduplicated.edx_module_id as resource_id,
    tutorbot_deduplicated.chatsession_thread_id as thread_id,
    tutorbot_deduplicated.human_message,
    tutorbot_deduplicated.agent_message as ai_message,
    tutorbot_deduplicated.chatsession_created_on as created_on,
    tutorbot_deduplicated.message_index
from tutorbot_deduplicated
left join chatbot on tutorbot_deduplicated.chatsession_thread_id = chatbot.chatsession_thread_id
where chatbot.chatsession_thread_id is null
