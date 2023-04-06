create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_basketrunselection__dbt_tmp

as (
    with source as (

        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__ecommerce_courserunselection

    )

    , renamed as (

        select
            id as basketrunselection_id
            , basket_id
            , run_id as courserun_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as basketrunselection_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as basketrunselection_updated_on
        from source

    )

    select * from renamed
);
