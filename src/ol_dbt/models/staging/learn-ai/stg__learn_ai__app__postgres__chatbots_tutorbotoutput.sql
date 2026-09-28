with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__ai_chatbots_tutorbotoutput') }}
)

{{ deduplicate_raw_table(raw_table='raw__learn_ai__app__postgres__ai_chatbots_tutorbotoutput', partition_columns = 'id') }}
, cleaned as (
    select
        id as tutorbotoutput_id
        , thread_id as chatsession_thread_id
        , chat_json as tutorbot_chat_json
    from most_recent_source
)

select * from cleaned
