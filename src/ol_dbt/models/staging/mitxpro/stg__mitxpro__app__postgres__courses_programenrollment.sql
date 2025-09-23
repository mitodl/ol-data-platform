-- xPro Program Enrollment Information
with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__courses_programenrollment") }}
    ),
    cleaned as (
        select
            id as programenrollment_id,
            change_status as programenrollment_enrollment_status,
            active as programenrollment_is_active,
            program_id,
            user_id,
            company_id as ecommerce_company_id,
            order_id as ecommerce_order_id,
            {{ cast_timestamp_to_iso8601("created_on") }} as programenrollment_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as programenrollment_updated_on
        from source
    )

select *
from cleaned
