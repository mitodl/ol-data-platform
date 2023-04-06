create table ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_electiveset__dbt_tmp

as (
    -- MicroMasters program elective set Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__micromasters__app__postgres__courses_electivesset
    )

    , cleaned as (
        select
            id as electiveset_id
            , program_id
            , title as electiveset_title
            , required_number as electiveset_required_number
        from source
    )

    select * from cleaned
);
