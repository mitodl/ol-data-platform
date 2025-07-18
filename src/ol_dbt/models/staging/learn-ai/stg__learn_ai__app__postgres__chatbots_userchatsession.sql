with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__ai_chatbots_userchatsession') }}
)

{{ deduplicate_raw_table(order_by='updated_on' , partition_columns = 'id') }}
, cleaned as (
    select
        id as chatsession_id
        , agent as chatsession_agent
        , title as chatsession_title
        , user_id
        , thread_id as chatsession_thread_id
        , object_id as chatsession_object_id
        , dj_session_key as chatsession_django_session_key
        , {{ cast_timestamp_to_iso8601('created_on') }} as chatsession_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as chatsession_updated_on
    from most_recent_source
)

select * from cleaned
