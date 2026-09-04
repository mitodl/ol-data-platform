with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_basketitem') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as basketitem_id
        , quantity as basketitem_quantity
        , basket_id
        , product_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as basketitem_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as basketitem_updated_on

    from most_recent_source

)

select * from renamed
