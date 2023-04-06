create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__courses__dbt_tmp

as (
    -- Course information for Bootcamps

    with courses as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_course
    )

    select
        course_id
        , course_title
    from courses
);
