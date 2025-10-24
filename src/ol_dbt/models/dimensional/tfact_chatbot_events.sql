{% set openedx_chatbot_events = (
    'ol_openedx_chat.drawer.open'
    , 'ol_openedx_chat.drawer.close'
    , 'ol_openedx_chat.drawer.submit'
    , 'ol_openedx_chat.drawer.response'
    , 'ol_openedx_chat.drawer.tabchange'
   )
%}
{% set canvas_chatbot_events = (
     'ol_openedx_chat_xblock.OLChat.submit'
    , 'ol_openedx_chat_xblock.OLChat.response'
   )
%}

with openedx_events as (
    select
        user_username
        , openedx_user_id
        , org_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , useractivity_session_id as session_id
        , json_query(useractivity_context_object, 'lax $.module.usage_key' omit quotes) as block_id
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as block_name
        , json_query(useractivity_event_object, 'lax $.value' omit quotes) as event_value
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ openedx_chatbot_events }}
)

, canvas_events as (
    select
        user_username
        , openedx_user_id
        , org_id
        , courserun_readable_id
        , useractivity_event_type as event_type
        , useractivity_event_object as event_json
        , useractivity_session_id as session_id
        , json_query(useractivity_context_object, 'lax $.module.usage_key' omit quotes) as block_id
        , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as block_name
        , json_query(useractivity_event_object, 'lax $.xblock_state' omit quotes) as chatbot_type
        , json_query(useractivity_event_object, 'lax $.problem_set' omit quotes) as problem_set
        , nullif(json_query(useractivity_event_object, 'lax $.canvas_course_id' omit quotes),'') as canvas_course_id
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
        , case when useractivity_event_type like '%response'
             then regexp_replace(
                 json_query(useractivity_event_object, 'lax $.value' omit quotes)
                 , '^b[''"]|[''"]$', ''
               )
          else json_query(useractivity_event_object, 'lax $.value' omit quotes)
        end as event_value
        , regexp_extract(
             json_query(useractivity_event_object, 'lax $.value' omit quotes)
             , '"thread_id": "([^"]+)"'
             , 1
        ) AS thread_id
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ canvas_chatbot_events }}
)

, learn_ai_userchatsession as (
    select * from {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }}
)

, users as (
    select * from {{ ref('dim_user') }}
)

, canvas_events_with_learnai as (
    select
        canvas_events.*
        , coalesce(canvas_events.canvas_course_id, learn_ai_userchatsession.chatsession_object_id) as canvas_object_id
    from canvas_events
    left join learn_ai_userchatsession
        on canvas_events.thread_id = learn_ai_userchatsession.chatsession_thread_id
)

--- this CTE fills in missing canvas_object_id values by looking ahead within the same session_id and block_id
--- to find the next event's canvas_object_id as only the response events have it set
, canvas_events_filled as (
    select
        *
       , coalesce(canvas_object_id, next_canvas_object_id) AS filled_canvas_object_id
    from (
        select
            *,
            lead(canvas_object_id)
                over (partition by session_id, block_id order by event_timestamp)
                as next_canvas_object_id
        from canvas_events_with_learnai
    )
)

, combined as (
    select
        'open_edx' as chatbot_source
            , user_username
            , openedx_user_id
            , org_id
            , courserun_readable_id
            , event_type
            , session_id
            , block_id
            , block_name
            , 'Tutor' as chatbot_type
            , event_value
            , event_timestamp
            , event_json
    from openedx_events

    union

    select
        'canvas' as chatbot_source
        , user_username
        , openedx_user_id
        , org_id
        , split_part(filled_canvas_object_id,  '+', 1) as courserun_readable_id
        , event_type
        , session_id
        , block_id
        , block_name
        , chatbot_type
        , event_value
        , event_timestamp
        , event_json
    from canvas_events_filled
)

select
    users.user_pk as user_fk
    , combined.chatbot_source
    , combined.user_username
    , combined.openedx_user_id
    , combined.org_id
    , combined.courserun_readable_id
    , combined.event_type
    , combined.session_id
    , combined.block_id
    , combined.block_name
    , combined.chatbot_type
    , combined.event_value
    , combined.event_timestamp
    , combined.event_json
from combined
left join users
    on combined.openedx_user_id = users.mitlearn_openedx_user_id
