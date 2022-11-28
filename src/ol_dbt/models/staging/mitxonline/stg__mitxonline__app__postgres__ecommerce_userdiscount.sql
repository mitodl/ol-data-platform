with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__ecommerce_userdiscount') }}

)

, renamed as (

    select
        id as userdiscount_id
        , user_id
        , discount_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as userdiscount_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as userdiscount_updated_on
    from source

)

select * from renamed
