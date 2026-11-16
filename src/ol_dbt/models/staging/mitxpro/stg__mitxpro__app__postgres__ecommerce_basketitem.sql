with source as (

    {{ live_raw_rows('raw__xpro__app__postgres__ecommerce_basketitem') }}

)

, renamed as (

    select
        id as basketitem_id
        , quantity as basketitem_quantity
        , basket_id
        , product_id
        , program_run_id as programrun_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as basketitem_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as basketitem_updated_on
    from source

)

select * from renamed
