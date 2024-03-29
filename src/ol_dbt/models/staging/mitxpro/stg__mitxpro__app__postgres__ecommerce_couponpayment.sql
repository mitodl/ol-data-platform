with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_couponpayment') }}

)

, renamed as (
    select
        id as couponpayment_id
        , name as couponpayment_name
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as couponpayment_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as couponpayment_created_on
    from source
)

select * from renamed
