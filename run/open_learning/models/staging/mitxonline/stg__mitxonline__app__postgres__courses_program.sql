create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_program__dbt_tmp

as (
    -- MITx Online Program Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__courses_program
    )

    , cleaned as (
        select
            id as program_id
            , live as program_is_live
            , title as program_title
            , readable_id as program_readable_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as program_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as program_updated_on
        from source
    )

    select * from cleaned
);
