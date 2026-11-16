with source as (

    {{ live_raw_rows('raw__xpro__app__postgres__ecommerce_company') }}

)

, renamed as (
    select
        id as company_id
        , name as company_name
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as company_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as company_created_on
    from source
)

select * from renamed
