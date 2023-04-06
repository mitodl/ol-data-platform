create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__course_blockedcountries__dbt_tmp

as (
    -- Course's Blocked countries information for MITx Online
    -- Keep it as separate model for flexibility to satisfy different use cases

    with blockedcountries as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_blockedcountry
    )

    select
        course_id
        , blockedcountry_code
    from blockedcountries
);
