create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_discountredemption__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__ecommerce_discountredemption

    )

    , renamed as (

        select
            id as discountredemption_id
            , redeemed_by_id as user_id
            , redeemed_order_id as order_id
            , redeemed_discount_id as discount_id
            ,
            to_iso8601(from_iso8601_timestamp(redemption_date))
            as discountredemption_timestamp

        from source

    )

    select * from renamed
);
