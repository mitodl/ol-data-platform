create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponproduct__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_couponeligibility

    )

    , renamed as (

        select
            id as couponproduct_id
            , product_id
            , coupon_id
            , program_run_id as programrun_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as couponproduct_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as couponproduct_updated_on
        from source

    )

    select * from renamed
);
