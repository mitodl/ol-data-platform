with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__flexiblepricing_currencyexchangerate') }}

)

, renamed as (

    select
        id as currencyexchangerate_id
        , description as currencyexchangerate_description
        , currency_code as currencyexchangerate_currency_code
        , exchange_rate as currencyexchangerate_exchange_rate
        , to_iso8601(
            from_iso8601_timestamp(created_on)
        ) as currencyexchangerate_created_on
        , to_iso8601(
            from_iso8601_timestamp(updated_on)
        ) as currencyexchangerate_updated_on

    from source

)

select * from renamed
