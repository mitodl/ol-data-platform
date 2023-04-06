create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__course_runs__dbt_tmp

as (
    -- Course Runs information for Bootcamps

    with runs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_courserun
    )

    select
        courserun_id
        , course_id
        , courserun_title
        , courserun_readable_id
        , courserun_start_on
        , courserun_end_on
    from runs
);
