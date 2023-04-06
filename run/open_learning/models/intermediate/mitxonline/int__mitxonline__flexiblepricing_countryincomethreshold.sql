create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_countryincomethreshold__dbt_tmp

as (
    with countryincomethreshold as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_countryincomethreshold
    )

    select
        countryincomethreshold_id
        , countryincomethreshold_created_on
        , countryincomethreshold_updated_on
        , countryincomethreshold_country_code
        , countryincomethreshold_income_threshold
    from countryincomethreshold
);
