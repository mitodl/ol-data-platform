create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_countryincomethreshold__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__flexiblepricing_countryincomethreshold

    )

    , renamed as (

        select
            id as countryincomethreshold_id
            , country_code as countryincomethreshold_country_code
            , income_threshold as countryincomethreshold_income_threshold
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as countryincomethreshold_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as countryincomethreshold_updated_on

        from source

    )

    select * from renamed
);
