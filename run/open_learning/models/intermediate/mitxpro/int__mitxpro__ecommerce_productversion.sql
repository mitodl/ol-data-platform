create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_productversion__dbt_tmp

as (
    with productversions as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_productversion
    )

    select
        productversion_id
        , productversion_readable_id
        , productversion_price
        , productversion_description
        , product_id
        , productversion_requires_enrollment_code
        , productversion_updated_on
        , productversion_created_on
    from productversions
);
