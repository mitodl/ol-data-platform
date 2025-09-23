-- xPro Course Run Enrollment Information
with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__courses_courserunenrollment") }}
    ),
    cleaned as (
        select
            id as courserunenrollment_id,
            change_status as courserunenrollment_enrollment_status,
            active as courserunenrollment_is_active,
            edx_enrolled as courserunenrollment_is_edx_enrolled,
            run_id as courserun_id,
            user_id,
            company_id as ecommerce_company_id,
            order_id as ecommerce_order_id,
            {{ cast_timestamp_to_iso8601("created_on") }} as courserunenrollment_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as courserunenrollment_updated_on
        from source
    )

select *
from cleaned
