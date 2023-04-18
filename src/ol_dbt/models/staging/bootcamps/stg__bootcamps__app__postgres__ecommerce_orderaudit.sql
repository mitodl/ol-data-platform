with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__ecommerce_orderaudit') }}

)

, renamed as (
    select
        id as orderaudit_id
        , order_id
        , acting_user_id as orderaudit_acting_user_id
        , data_before as orderaudit_data_before
        , data_after as orderaudit_data_after
        ,{{ cast_timestamp_to_iso8601('created_on') }} as orderaudit_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as orderaudit_updated_on
    from source

)

select * from renamed
