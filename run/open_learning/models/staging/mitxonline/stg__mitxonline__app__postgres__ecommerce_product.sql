create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__ecommerce_product__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__ecommerce_product

    )

    , renamed as (

        select
            id as product_id
            , price as product_price
            , is_active as product_is_active
            , object_id as product_object_id
            , description as product_description
            , content_type_id as contenttype_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as product_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as product_updated_on
        from source
    )

    select * from renamed
);
