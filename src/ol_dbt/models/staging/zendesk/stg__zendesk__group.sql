with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__thirdparty__zendesk_support__groups") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        select
            id as group_id,
            url as group_api_url,
            name as group_name,
            description as group_description,
            default as group_is_default,
            deleted as group_is_deleted,
            is_public as group_is_public,
            {{ cast_timestamp_to_iso8601("created_at") }} as group_created_at,
            {{ cast_timestamp_to_iso8601("updated_at") }} as group_updated_at
        from most_recent_source
    )

select *
from cleaned
