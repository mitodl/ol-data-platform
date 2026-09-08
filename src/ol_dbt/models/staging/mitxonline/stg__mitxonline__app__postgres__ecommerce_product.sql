with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_product') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as product_id
        , cast(price as decimal(38, 2)) as product_price
        , is_active as product_is_active
        , object_id as product_object_id
        , description as product_description
        , content_type_id as contenttype_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as product_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as product_updated_on
    from most_recent_source
)

select * from renamed
