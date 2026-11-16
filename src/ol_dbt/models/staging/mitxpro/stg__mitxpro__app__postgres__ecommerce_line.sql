with source as (

    {{ live_raw_rows('raw__xpro__app__postgres__ecommerce_line') }}

)

, renamed as (

    select
        id as line_id
        , order_id
        , product_version_id as productversion_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as line_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as line_updated_on
    from source

)

select * from renamed
