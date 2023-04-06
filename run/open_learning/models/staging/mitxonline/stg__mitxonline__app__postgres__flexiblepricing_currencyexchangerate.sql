create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_currencyexchangerate__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__flexiblepricing_currencyexchangerate

    )

    , renamed as (

        select
            id as currencyexchangerate_id
            , description as currencyexchangerate_description
            , currency_code as currencyexchangerate_currency_code
            , exchange_rate as currencyexchangerate_exchange_rate
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as currencyexchangerate_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as currencyexchangerate_updated_on

        from source

    )

    select * from renamed
);
