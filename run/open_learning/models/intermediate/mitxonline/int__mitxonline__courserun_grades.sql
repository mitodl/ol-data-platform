create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__courserun_grades__dbt_tmp

as (
    -- Course Grade information for MITx Online

    with grades as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_courserungrade
    )

    , runs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_courserun
    )

    , users as (
        select * from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__users_user
    )

    , courserun_grades as (
        select
            grades.courserungrade_id
            , grades.courserun_id
            , runs.course_id
            , runs.courserun_title
            , runs.courserun_readable_id
            , runs.courserun_url
            , grades.courserungrade_grade
            , grades.courserungrade_letter_grade
            , grades.courserungrade_is_passing
            , grades.courserungrade_created_on
            , grades.courserungrade_updated_on
            , grades.user_id
            , users.user_username
            , users.user_email
        from grades
        inner join runs on grades.courserun_id = runs.courserun_id
        inner join users on grades.user_id = users.user_id
    )

    select * from courserun_grades
);
