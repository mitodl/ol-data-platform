with source as (

    {{ live_raw_rows('raw__micromasters__app__postgres__ecommerce_couponinvoice') }}

)

, renamed as (

    select
        id as couponinvoice_id
        , invoice_number as couponinvoice_number
        , description as couponinvoice_description
    from source

)

select * from renamed
