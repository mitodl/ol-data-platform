with chatbot as (
    select * from {{ ref('int__learn_ai__chatbot') }}
)

, checkpoint_turns as (
    select
        cast(chatbot.djangocheckpoint_id as varchar) as source_record_ref
        , chatbot.chatsession_thread_id
        , chatbot.chatsession_agent
        , chatbot.chatsession_title
        , chatbot.chatsession_object_id
        , chatbot.user_global_id
        , chatbot.human_message
        , chatbot.rating as explicit_rating
        , chatbot.checkpoint_source
        , chatbot.checkpoint_type
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

-- Pre-checkpoint history, same pattern as chatbot_usage_report.sql.
, tutorbot as (
    select
        *
        , json_parse(json_extract_scalar(tutorbot_chat_json, '$')) as chat_json
    from {{ ref('int__learn_ai__tutorbot') }}
)

, tutorbot_flatten as (
    select
        tutorbot.tutorbotoutput_id
        , tutorbot.chatsession_thread_id
        , tutorbot.chatsession_agent
        , tutorbot.chatsession_title
        , tutorbot.edx_module_id
        , tutorbot.courserun_readable_id
        , tutorbot.user_global_id
        , tutorbot.chatsession_created_on
        , tutorbot.chatsession_updated_on
        , t.idx as message_index
        , case
            when json_extract_scalar(t.element, '$.type') = 'HumanMessage'
                then json_extract_scalar(t.element, '$.content')
        end as human_message
    from tutorbot
    cross join
        unnest(cast(json_extract(tutorbot.chat_json, '$.chat_history') as array<json>))
        with ordinality as t(element, idx) -- noqa: PRS
)

, tutorbot_deduplicated as (
    select
        tutorbotoutput_id
        , chatsession_thread_id
        , chatsession_agent
        , chatsession_title
        , edx_module_id
        , courserun_readable_id
        , user_global_id
        , chatsession_created_on
        , chatsession_updated_on
        , human_message
        , message_index
    from (
        select
            *
            , row_number() over (
                partition by chatsession_thread_id, human_message
                order by tutorbotoutput_id, message_index
            ) as message_occurrence
        from tutorbot_flatten
        where human_message is not null
    )
    where message_occurrence = 1
)

, tutorbot_turns as (
    select
        cast(tutorbot_deduplicated.tutorbotoutput_id as varchar)
            || '-' || cast(tutorbot_deduplicated.message_index as varchar)
            as source_record_ref
        , tutorbot_deduplicated.chatsession_thread_id
        , tutorbot_deduplicated.chatsession_agent
        , tutorbot_deduplicated.chatsession_title
        , tutorbot_deduplicated.edx_module_id as chatsession_object_id
        , tutorbot_deduplicated.user_global_id
        , tutorbot_deduplicated.human_message
        , cast(null as varchar) as explicit_rating
        , cast(null as varchar) as checkpoint_source
        , cast(null as varchar) as checkpoint_type
        , tutorbot_deduplicated.chatsession_created_on as occurred_at
        , tutorbot_deduplicated.chatsession_updated_on
        , tutorbot_deduplicated.courserun_readable_id
        , row_number() over (
            partition by tutorbot_deduplicated.chatsession_thread_id
            order by tutorbot_deduplicated.message_index
        ) as turn_index
    from tutorbot_deduplicated
    left join chatbot
        on tutorbot_deduplicated.chatsession_thread_id = chatbot.chatsession_thread_id
    where chatbot.chatsession_thread_id is null
)

, human_turns as (
    select * from checkpoint_turns
    union all
    select * from tutorbot_turns
)

-- courserun_readable_id alone isn't a unique key on dim_course_run (its surrogate
-- key is platform + courserun_readable_id), so pick one deterministically rather
-- than fan out a turn across every platform sharing that readable_id.
, course_run as (
    select
        courserun_readable_id
        , platform
        , row_number() over (
            partition by courserun_readable_id order by platform
        ) as platform_rank
    from {{ ref('dim_course_run') }}
)

select
    'learn_ai_tutor' as source_slug
    , human_turns.occurred_at
    , human_turns.source_record_ref
    , human_turns.human_message as text
    , human_turns.chatsession_title as title
    , human_turns.chatsession_thread_id as conversation_ref
    , human_turns.turn_index
    , human_turns.turn_index = 1 as is_conversation_opening
    , human_turns.user_global_id as subject_user_ref
    , cast(null as varchar) as source_url
    , 'chat' as channel_slug
    , human_turns.courserun_readable_id
    , course_run.platform
    , case human_turns.chatsession_agent
        when 'TutorBot' then 'courseware_block'
        when 'VideoGPTBot' then 'courseware_block'
        when 'SyllabusBot' then 'course'
        when 'CanvasSyllabusBot' then 'course'
        when 'ResourceRecommendationBot' then 'resource'
    end as subject_type
    , human_turns.chatsession_object_id as subject_ref
    , cast(null as varchar) as subject_url
    , human_turns.explicit_rating
    , human_turns.occurred_at as created_at
    , human_turns.chatsession_updated_on as updated_at
    , json_object(
        'chatsession_agent': human_turns.chatsession_agent
        , 'checkpoint_source': human_turns.checkpoint_source
        , 'checkpoint_type': human_turns.checkpoint_type
    ) as source_metadata
from human_turns
left join course_run
    on human_turns.courserun_readable_id = course_run.courserun_readable_id
    and course_run.platform_rank = 1
