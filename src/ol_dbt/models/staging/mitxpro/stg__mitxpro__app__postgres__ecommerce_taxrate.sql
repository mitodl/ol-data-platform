with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_taxrate') }}

)

, renamed as (

    select
        id as taxrate_id
        , country_code as taxrate_country_code
        , cast(tax_rate as decimal(38, 2)) as taxrate_tax_rate
        , tax_rate_name as taxrate_tax_rate_name
        , active as taxrate_is_active
        ,{{ cast_timestamp_to_iso8601('created_on') }} as taxrate_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as taxrate_updated_on
    from source

)

select * from renamed
