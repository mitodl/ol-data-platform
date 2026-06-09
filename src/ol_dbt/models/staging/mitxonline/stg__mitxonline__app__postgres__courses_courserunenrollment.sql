-- MITx Online Course Run Enrollment Information
-- Django enforces unique_together on (user, run), but hard-deletes followed by re-enrollment
-- produce two rows with different IDs but the same (user_id, run_id) in the raw Iceberg table.
-- We deduplicate here, preferring active=True records first, then the highest id (most recently
-- created) as a tiebreaker. This correctly handles cases where the higher-id record is a stale
-- inactive/unenrolled row rather than the current live enrollment.
with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitxonline__app__postgres__courses_courserunenrollment") }}
    )

    {{ deduplicate_raw_table(order_by="active desc, id", partition_columns="user_id, run_id") }},
    cleaned as (
        select
            id as courserunenrollment_id,
            -- -since raw data has both empty string and null, convert them to null for consistency
            active as courserunenrollment_is_active,
            edx_enrolled as courserunenrollment_is_edx_enrolled,
            run_id as courserun_id,
            user_id,
            enrollment_mode as courserunenrollment_enrollment_mode,
            edx_emails_subscription as courserunenrollment_has_edx_email_subscription,
            case when change_status = '' then null else change_status end as courserunenrollment_enrollment_status,
            {{ cast_timestamp_to_iso8601("created_on") }} as courserunenrollment_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as courserunenrollment_updated_on
        from most_recent_source
    )

select *
from cleaned
