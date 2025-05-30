with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_couponselection') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'basket_id') }}
, renamed as (

    select
        id as couponbasket_id
        , basket_id
        , coupon_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as couponbasket_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as couponbasket_updated_on
    from most_recent_source

)

select * from renamed
