with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__b2b_ecommerce_b2border') }}

)

, renamed as (

    select
        id as b2border_id
        , total_price as b2border_total_price
        , status as b2border_status
        , per_item_price as b2border_per_item_price
        , unique_id as b2border_unique_uuid
        , num_seats as b2border_num_seats
        , coupon_id as b2bcoupon_id
        , product_version_id as productversion_id
        , coupon_payment_version_id as couponpaymentversion_id
        , contract_number as b2border_contract_number
        , discount as b2border_discount
        , program_run_id as programrun_id
        , email as b2border_email
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as b2border_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as b2border_created_on
    from source

)

select * from renamed
