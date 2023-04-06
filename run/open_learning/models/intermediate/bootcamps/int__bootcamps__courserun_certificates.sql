create table ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__courserun_certificates__dbt_tmp

as (
    -- Course Certificate information for Bootcamps

    with certificates as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_courseruncertificate
    )

    , runs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_courserun
    )

    , users as (
        select * from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__auth_user
    )

    , bootcamps_certificates as (
        select
            certificates.courseruncertificate_id
            , certificates.courseruncertificate_uuid
            , certificates.courserun_id
            , runs.courserun_title
            , runs.courserun_readable_id
            , runs.course_id
            , certificates.courseruncertificate_is_revoked
            , certificates.courseruncertificate_created_on
            , certificates.courseruncertificate_updated_on
            , certificates.user_id
            , users.user_username
            , users.user_email
        from certificates
        inner join runs on certificates.courserun_id = runs.courserun_id
        inner join users on certificates.user_id = users.user_id
    )

    select * from bootcamps_certificates
);
