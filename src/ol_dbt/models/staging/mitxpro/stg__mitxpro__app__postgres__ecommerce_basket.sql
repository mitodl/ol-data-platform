with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_basket') }}

)

, renamed as (

    select
        id as basket_id
        , user_id
        , to_iso8601(from_iso8601_timestamp(created_on)) as basket_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on)) as basket_updated_on

    from source

)

select * from renamed
