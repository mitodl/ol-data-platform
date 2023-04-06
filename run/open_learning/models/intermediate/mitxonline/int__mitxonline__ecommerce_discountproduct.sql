create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_discountproduct__dbt_tmp

as (
    with discountproduct as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_discountproduct
    )

    select
        discountproduct_id
        , discountproduct_created_on
        , discountproduct_updated_on
        , product_id
        , discount_id
    from discountproduct
);
