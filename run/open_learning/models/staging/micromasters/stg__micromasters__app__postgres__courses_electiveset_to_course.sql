create table ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_electiveset_to_course__dbt_tmp

as (
    -- MicroMasters course to electives set Information

    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__micromasters__app__postgres__courses_electivecourse
    )

    , cleaned as (
        select
            id as electivesettocourse_id
            , course_id
            , electives_set_id as electiveset_id
        from source
    )

    select * from cleaned
);
