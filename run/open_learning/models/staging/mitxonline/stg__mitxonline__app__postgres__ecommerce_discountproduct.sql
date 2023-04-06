create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_discountproduct__dbt_tmp

as (
    with source as (

        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__ecommerce_discountproduct

    )

    , renamed as (

        select
            id as discountproduct_id
            , product_id
            , discount_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as discountproduct_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as discountproduct_updated_on

        from source

    )

    select * from renamed
);
