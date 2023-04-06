create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__b2becommerce_b2bcouponaudit__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__b2b_ecommerce_b2bcouponaudit

    )

    , renamed as (

        select
            id as b2bcouponaudit_id
            , coupon_id as b2bcoupon_id
            , acting_user_id as b2bcouponaudit_acting_user_id
            , data_before as b2bcouponaudit_data_before
            , data_after as b2bcouponaudit_data_after
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as b2bcouponaudit_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as b2bcouponaudit_updated_on
        from source

    )

    select * from renamed
);
