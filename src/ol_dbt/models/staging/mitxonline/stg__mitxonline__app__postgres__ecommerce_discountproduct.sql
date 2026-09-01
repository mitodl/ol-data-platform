with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_discountproduct') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as discountproduct_id
        , product_id
        , discount_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as discountproduct_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as discountproduct_updated_on

    from most_recent_source

)

select * from renamed
