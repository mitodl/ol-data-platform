create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_discountredemption__dbt_tmp

as (
    with discountredemption as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_discountredemption
    )


    select
        discountredemption_id
        , user_id
        , discountredemption_timestamp
        , order_id
        , discount_id
    from discountredemption
);
