create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__courses__dbt_tmp

as (
    -- Course information for MITx Online

    with courses as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_course
    )

    select
        course_id
        , course_title
        , course_is_live
        , course_readable_id
    from courses
);
