create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__ecommerce_userdiscount__dbt_tmp

as (
    with userdiscount as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_userdiscount
    )

    select
        userdiscount_id
        , userdiscount_created_on
        , userdiscount_updated_on
        , user_id
        , discount_id
    from userdiscount
);
