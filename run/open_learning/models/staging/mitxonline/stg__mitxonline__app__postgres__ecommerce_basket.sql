create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_basket__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__ecommerce_basket

    )

    , renamed as (

        select
            id as basket_id
            , user_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as basket_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as basket_updated_on

        from source

    )

    select * from renamed
);
