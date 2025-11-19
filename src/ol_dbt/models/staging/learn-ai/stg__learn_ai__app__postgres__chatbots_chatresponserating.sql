with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__learn_ai__app__postgres__ai_chatbots_chatresponserating') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'id') }}
, cleaned as (
    select
        checkpoint_id as djangocheckpoint_id
        , rating
        , rating_reason
        , {{ cast_timestamp_to_iso8601('created_on') }} as rating_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as rating_updated_on
    from most_recent_source
)

select * from cleaned
