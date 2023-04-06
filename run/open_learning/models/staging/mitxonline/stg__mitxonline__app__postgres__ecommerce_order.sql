create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_order__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__ecommerce_order

    )

    , renamed as (

        select
            id as order_id
            , state as order_state
            , purchaser_id as order_purchaser_user_id
            , reference_number as order_reference_number
            , total_price_paid as order_total_price_paid
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as order_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as order_updated_on
        from source

    )

    select * from renamed
);
