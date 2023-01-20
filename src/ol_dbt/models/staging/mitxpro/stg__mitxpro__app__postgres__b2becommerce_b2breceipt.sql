with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__b2b_ecommerce_b2breceipt') }}

)

, renamed as (

    select
        id as b2breceipt_id
        , order_id as b2border_id
        , data as b2breceipt_data
        , {{ cast_timestamp_to_iso8601('created_on') }} as b2breceipt_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as b2breceipt_updated_on
    from source

)

select * from renamed
