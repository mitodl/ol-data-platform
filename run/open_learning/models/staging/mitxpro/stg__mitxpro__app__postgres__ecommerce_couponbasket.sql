create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponbasket__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_couponselection

    )

    , renamed as (

        select
            id as couponbasket_id
            , basket_id
            , coupon_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as couponbasket_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as couponbasket_updated_on
        from source

    )

    select * from renamed
);
