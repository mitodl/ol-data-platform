with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__certificates_generatedcertificate") }}
    )

    {{ deduplicate_raw_table(order_by="modified_date", partition_columns="id") }}
select
    id,
    user_id,
    course_id,
    key,
    name,
    distinction,
    verify_uuid,
    download_uuid,
    download_url,
    grade,
    created_date,
    modified_date,
    mode,
    status,
    error_reason
from most_recent_source
