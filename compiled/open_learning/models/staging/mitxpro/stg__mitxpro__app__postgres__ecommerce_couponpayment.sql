with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_couponpayment

)

, renamed as (
    select
        id as couponpayment_id
        , name as couponpayment_name
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as couponpayment_updated_on
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as couponpayment_created_on
    from source
)

select * from renamed
