with source as (

    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__flexiblepricing_countryincomethreshold') }}

)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') }}

, renamed as (

    select
        id as countryincomethreshold_id
        , country_code as countryincomethreshold_country_code
        , income_threshold as countryincomethreshold_income_threshold
        ,{{ cast_timestamp_to_iso8601('created_on') }} as countryincomethreshold_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as countryincomethreshold_updated_on

    from most_recent_source

)

select * from renamed
