with source as (

    select * from dev.main_raw.raw__mitxonline__app__postgres__ecommerce_basketitem

)

, renamed as (

    select
        id as basketitem_id
        , quantity as basketitem_quantity
        , basket_id
        , product_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as basketitem_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as basketitem_updated_on

    from source

)

select * from renamed
