create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__ecommerce_order__dbt_tmp

as (
    with orders as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_order
    )

    , lines as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__ecommerce_line
    )

    select
        orders.order_id
        , orders.order_state
        , orders.order_purchaser_user_id
        , orders.application_id
        , orders.order_payment_type
        , orders.order_total_price_paid
        , orders.order_created_on
        , orders.order_updated_on
        , lines.line_id
        , lines.line_description
        , lines.courserun_id
    from lines
    inner join orders on orders.order_id = lines.order_id
);
