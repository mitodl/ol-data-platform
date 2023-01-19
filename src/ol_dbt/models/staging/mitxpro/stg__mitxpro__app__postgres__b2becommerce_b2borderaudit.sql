with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__b2b_ecommerce_b2borderaudit') }}

)

, renamed as (

    select
        id as b2borderaudit_id
        , order_id as b2border_id
        , acting_user_id as b2borderaudit_acting_user_id
        , data_before as b2borderaudit_data_before
        , data_after as b2borderaudit_data_after
        , {{ cast_timestamp_to_iso8601('created_on') }} as b2borderaudit_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as b2borderaudit_updated_on
    from source

)

select * from renamed
