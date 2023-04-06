create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__courserun_certificates__dbt_tmp

as (
    -- Course Certificate information for MIT xPro

    with certificates as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_courseruncertificate
    )

    , runs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_courserun
    )

    , users as (
        select * from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__users_user
    )

    , courserun_certificates as (
        select
            certificates.courseruncertificate_id
            , certificates.courseruncertificate_uuid
            , certificates.courserun_id
            , runs.courserun_title
            , runs.courserun_readable_id
            , runs.courserun_url
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


    select * from courserun_certificates
);
