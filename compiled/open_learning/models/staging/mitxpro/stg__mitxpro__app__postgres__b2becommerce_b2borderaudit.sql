with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__b2b_ecommerce_b2borderaudit

)

, renamed as (

    select
        id as b2borderaudit_id
        , order_id as b2border_id
        , acting_user_id as b2borderaudit_acting_user_id
        , data_before as b2borderaudit_data_before
        , data_after as b2borderaudit_data_after
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as b2borderaudit_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as b2borderaudit_updated_on
    from source

)

select * from renamed
