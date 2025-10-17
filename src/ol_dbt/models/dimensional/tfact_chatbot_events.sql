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
        , json_query(useractivity_event_object, 'lax $.value' omit quotes) as event_value
        , json_query(useractivity_event_object, 'lax $.xblock_state' omit quotes) as chatbot_type
        , json_query(useractivity_event_object, 'lax $.problem_set' omit quotes) as problem_set
        , json_query(useractivity_event_object, 'lax $.canvas_course_id' omit quotes) as canvas_course_id
        , from_iso8601_timestamp_nanos(useractivity_timestamp) as event_timestamp
        , if (useractivity_event_type like '%submit'
          , json_query(useractivity_event_object, 'lax $.value' omit quotes)
        ) as human_message
        , if (useractivity_event_type like '%response'
          , json_query(useractivity_event_object, 'lax $.value' omit quotes)
        ) as agent_message
    from {{ ref('stg__mitxonline__canvas__tracking_logs__user_activity') }}
    where
        courserun_readable_id is not null
        and useractivity_event_type in {{ canvas_chatbot_events }}
)
