with chatbot_events as (
    select * from {{ ref('tfact_chatbot_events') }}
)

, course_content as (
    select * from {{ ref('dim_course_content') }}
)

, user as (
    select * from {{ ref('dim_user') }}
)

select 
    user.email as user_email
    , cast(chatbot_events.event_timestamp as date) as activity_date
    , chatbot_events.courserun_readable_id
    , count(distinct chatbot_events.session_id || chatbot_events.block_id) as chatbot_used_count
    , c.block_category
    , c.block_title
    , section.block_title as section_title
    , subsection.block_title as subsection_title
    , chatbot_events.chatbot_type
from chatbot_events
inner join user
    on chatbot_events.user_fk = user.user_pk
left join course_content as c
    on 
        chatbot_events.block_id = c.block_id
        and c.is_latest = true
left join course_content as section
    on 
        c.chapter_block_id = section.block_id
        and section.is_latest = true
left join course_content as subsection
    on 
        c.sequential_block_id = subsection.block_id
        and subsection.is_latest = true
where chatbot_events.event_type = 'ol_openedx_chat.drawer.submit'
group by     
    user.email 
    , cast(chatbot_events.event_timestamp as date) 
    , chatbot_events.courserun_readable_id
    , c.block_category
    , c.block_title
    , section.block_title
    , subsection.block_title
    , chatbot_events.chatbot_type