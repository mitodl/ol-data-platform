with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__ecommerce_bulkcouponassignment") }}

    ),
    renamed as (

        select
            assignment_sheet_id as bulkcouponassignment_assignment_sheet_id,
            id as bulkcouponassignment_id,
            {{ cast_timestamp_to_iso8601("assignments_started_date") }} as bulkcouponassignment_assignments_started_on,
            {{ cast_timestamp_to_iso8601("last_assignment_date") }} as bulkcouponassignment_last_assignment_on,
            {{ cast_timestamp_to_iso8601("message_delivery_completed_date") }}
            as bulkcouponassignment_message_delivery_completed_on,
            {{ cast_timestamp_to_iso8601("sheet_last_modified_date") }} as bulkcouponassignment_sheet_last_modified_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as bulkcouponassignment_updated_on,
            {{ cast_timestamp_to_iso8601("created_on") }} as bulkcouponassignment_created_on

        from source

    )

select *
from renamed
