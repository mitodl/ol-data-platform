create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__course_runs__dbt_tmp

as (
    -- Course Runs information for MITxPro

    with course_runs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_courserun
    )

    select
        courserun_id
        , course_id
        , courserun_title
        , courserun_readable_id
        , courserun_tag
        , courserun_url
        , courserun_start_on
        , courserun_end_on
        , courserun_enrollment_start_on
        , courserun_enrollment_end_on
        , courserun_is_live
    from course_runs
);
