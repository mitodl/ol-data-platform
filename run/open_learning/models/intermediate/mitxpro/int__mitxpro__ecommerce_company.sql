create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_company__dbt_tmp

as (
    with company as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_company
    )

    select
        company_id
        , company_name
        , company_updated_on
        , company_created_on
    from company
);
