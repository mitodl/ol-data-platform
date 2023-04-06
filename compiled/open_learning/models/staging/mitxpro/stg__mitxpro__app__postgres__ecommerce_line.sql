with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__ecommerce_line

)

, renamed as (

    select
        id as line_id
        , order_id
        , product_version_id as productversion_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as line_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as line_updated_on
    from source

)

select * from renamed
