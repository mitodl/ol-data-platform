create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_order__dbt_tmp

as (
    with source as (

        select * from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_order

    )

    , renamed as (
        select
            id as order_id
            , status as order_state
            , total_price_paid as order_total_price_paid
            , purchaser_id as order_purchaser_user_id
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as order_updated_on
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as order_created_on
        from source
    )

    select * from renamed
);
