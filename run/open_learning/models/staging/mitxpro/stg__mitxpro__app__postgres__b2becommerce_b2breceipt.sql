create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__b2becommerce_b2breceipt__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__b2b_ecommerce_b2breceipt

    )

    , renamed as (

        select
            id as b2breceipt_id
            , order_id as b2border_id
            , data as b2breceipt_data
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as b2breceipt_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as b2breceipt_updated_on
        from source

    )

    select * from renamed
);
