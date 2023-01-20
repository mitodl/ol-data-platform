with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_userdiscount') }}

)

, renamed as (

    select
        id as userdiscount_id
        , user_id
        , discount_id
        , {{ cast_timestamp_to_iso8601('created_on') }} as userdiscount_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as userdiscount_updated_on
    from source

)

select * from renamed
