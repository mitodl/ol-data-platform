create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_line__dbt_tmp

as (
    with lines as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_line
    )

    , productversions as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_productversion
    )

    , products as (
        select *
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_product
    )

    , programrunlines as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_programrunline
    )

    select
        lines.line_id
        , lines.order_id
        , lines.productversion_id
        , lines.line_created_on
        , lines.line_updated_on
        , products.product_id
        , products.courserun_id
        , products.program_id
        , products.product_type
        , programrunlines.programrun_id
    from lines
    inner join productversions on productversions.productversion_id = lines.productversion_id
    inner join products on products.product_id = productversions.product_id
    left join programrunlines on programrunlines.line_id = lines.line_id
);
