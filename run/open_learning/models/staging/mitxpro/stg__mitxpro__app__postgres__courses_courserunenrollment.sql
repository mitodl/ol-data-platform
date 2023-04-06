create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_courserunenrollment__dbt_tmp

as (
    -- xPro Course Run Enrollment Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__courses_courserunenrollment
    )

    , cleaned as (
        select
            id as courserunenrollment_id
            , change_status as courserunenrollment_enrollment_status
            , active as courserunenrollment_is_active
            , edx_enrolled as courserunenrollment_is_edx_enrolled
            , run_id as courserun_id
            , user_id
            , company_id as ecommerce_company_id
            , order_id as ecommerce_order_id
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
