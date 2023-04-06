with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_orderaudit

)

, renamed as (

    select
        id as orderaudit_id
        , order_id
        , acting_user_id as orderaudit_acting_user_id
        , data_before as orderaudit_data_before
        , data_after as orderaudit_data_after
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as orderaudit_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as orderaudit_updated_on
    from source

)

select * from renamed
