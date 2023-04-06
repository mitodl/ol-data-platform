create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_courserunenrollment__dbt_tmp

as (
    -- MITx Online Course Run Enrollment Information

    with source as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__courses_courserunenrollment
    )

    , cleaned as (
        select
            id as courserunenrollment_id
            , change_status as courserunenrollment_enrollment_status
            , active as courserunenrollment_is_active
            , edx_enrolled as courserunenrollment_is_edx_enrolled
            , run_id as courserun_id
            , user_id
            , enrollment_mode as courserunenrollment_enrollment_mode
            , edx_emails_subscription as courserunenrollment_has_edx_email_subscription
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as courserunenrollment_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as courserunenrollment_updated_on
        from source
    )

    select * from cleaned
);
