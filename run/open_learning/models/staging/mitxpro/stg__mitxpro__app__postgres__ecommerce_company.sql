create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_company__dbt_tmp

as (
    with source as (

        select * from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_company

    )

    , renamed as (
        select
            id as company_id
            , name as company_name
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as company_updated_on
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as company_created_on
        from source
    )

    select * from renamed
);
