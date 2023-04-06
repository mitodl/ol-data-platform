create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_blockedcountry__dbt_tmp

as (
    -- MITx Online Course Blocked country Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__courses_blockedcountry
    )

    , cleaned as (
        select
            id as blockedcountry_id
            , country as blockedcountry_code
            , course_id
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as blockedcountry_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as blockedcountry_updated_on
        from source
    )

    select * from cleaned
);
