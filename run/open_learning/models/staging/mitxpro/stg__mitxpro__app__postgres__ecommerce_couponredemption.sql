create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_couponredemption__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_couponredemption

    )

    , renamed as (

        select
            id as couponredemption_id
            , order_id
            , coupon_version_id as couponversion_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as couponredemption_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as couponredemption_updated_on
        from source

    )

    select * from renamed
);
