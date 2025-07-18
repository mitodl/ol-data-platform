with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__ai_chatbots_tutorbotoutput') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as turorbotoutput_id
        , thread_id as chatsession_thread_id
        , chat_json as turorbot_chat_json
    from most_recent_source
)

select * from cleaned
