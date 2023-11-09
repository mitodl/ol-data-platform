with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__ecommerce_order') }}

)

, renamed as (
    select
        id as order_id
        , status as order_state
        , user_id as order_purchaser_user_id
        , application_id as application_id
        , payment_type as order_payment_type
        , cast(total_price_paid as decimal(38, 2)) as order_total_price_paid
        , concat('BOOTCAMP-prod-', cast(id as varchar)) as order_reference_number
        ,{{ cast_timestamp_to_iso8601('created_on') }} as order_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as order_updated_on
    from source

)

select * from renamed
