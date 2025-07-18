with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__ai_chatbots_djangocheckpoint') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        id as djangocheckpoint_id
        , type as checkpoint_type
        , session_id as chatsession_id
        , thread_id as chatsession_thread_id
        , checkpoint as checkpoint_json
        , checkpoint_ns as checkpoint_namespace
        , checkpoint_id
        , parent_checkpoint_id
        , metadata as checkpoint_metadata
    from most_recent_source
)

select * from cleaned
