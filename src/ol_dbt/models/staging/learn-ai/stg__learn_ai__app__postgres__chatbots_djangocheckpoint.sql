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
        , cast(json_query(metadata, 'lax $.step' omit quotes) as integer) as checkpoint_step
        , json_query(metadata, 'lax $.writes.__start__.messages.kwargs.content' omit quotes) as human_message
        , json_query(metadata, 'lax $.writes.agent.messages.kwargs.content' omit quotes) as agent_message
        , json_query(metadata, 'lax $.source' omit quotes) as checkpoint_source
        , {{ cast_timestamp_to_iso8601('created_on') }} as checkpoint_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as checkpoint_updated_on
    from most_recent_source
)

select * from cleaned
