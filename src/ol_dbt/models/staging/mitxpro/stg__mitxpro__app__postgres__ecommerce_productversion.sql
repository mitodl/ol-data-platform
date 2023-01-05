with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_productversion') }}

)

, renamed as (

    select
        id as productversion_id
        , text_id as productversion_readable_id
        , price as productversion_price
        , description as productversion_description
        , product_id
        , requires_enrollment_code as productversion_requires_enrollment_code
        , to_iso8601(
            from_iso8601_timestamp(updated_on)
        ) as productversion_updated_on
        , to_iso8601(
            from_iso8601_timestamp(created_on)
        ) as productversion_created_on
    from source

)

select * from renamed
