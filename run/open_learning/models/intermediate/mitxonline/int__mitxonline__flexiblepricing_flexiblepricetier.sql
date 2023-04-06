create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_flexiblepricetier__dbt_tmp

as (
    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
    )

    select
        flexiblepricetier_id
        , flexiblepricetier_is_current
        , flexiblepricetier_created_on
        , flexiblepricetier_updated_on
        , discount_id
        , courseware_object_id
        , flexiblepricetier_income_threshold_usd
        , contenttype_id

    from source
);
