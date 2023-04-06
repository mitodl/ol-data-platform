with source as (

    select * from dev.main_raw.raw__mitxonline__app__postgres__ecommerce_discountproduct

)

, renamed as (

    select
        id as discountproduct_id
        , product_id
        , discount_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as discountproduct_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as discountproduct_updated_on

    from source

)

select * from renamed
