with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_order') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as order_id
        , state as order_state
        , purchaser_id as order_purchaser_user_id
        , reference_number as order_reference_number
        , cast(total_price_paid as decimal(38, 2)) as order_total_price_paid
        ,{{ cast_timestamp_to_iso8601('created_on') }} as order_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as order_updated_on
    from most_recent_source

)

select * from renamed
