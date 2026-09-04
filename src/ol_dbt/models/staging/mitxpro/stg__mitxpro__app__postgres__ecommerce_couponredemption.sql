with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_couponredemption') }}

)

{{ deduplicate_raw_table(order_by="id", partition_columns="order_id, coupon_version_id") }}
, renamed as (

    select
        id as couponredemption_id
        , order_id
        , coupon_version_id as couponversion_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as couponredemption_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as couponredemption_updated_on
    from most_recent_source

)

select * from renamed
