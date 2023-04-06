with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__b2b_ecommerce_b2breceipt

)

, renamed as (

    select
        id as b2breceipt_id
        , order_id as b2border_id
        , data as b2breceipt_data
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as b2breceipt_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as b2breceipt_updated_on
    from source

)

select * from renamed
